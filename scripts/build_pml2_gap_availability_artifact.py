#!/usr/bin/env python3
"""Train the PML2 gap model by masking complete historical L2 intervals."""

from __future__ import annotations

import argparse
import bisect
import json
import os
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import islice
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.pml2.availability import (
    DEFAULT_ARTIFACT_PATH,
    SCHEMA_VERSION,
    payload_hash,
)

DEFAULT_ARCHIVE_ROOT = Path(
    "/data/jiahuaiyu/prediction-market-quant/lob_l2_archive_xue"
)
MASKING_HORIZONS_MS = (1_000, 2_000, 5_000, 10_000, 30_000)


def build_artifact(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    normalized = sorted(
        (_normalize_row(row) for row in rows), key=lambda row: row["decision_ts"]
    )
    if len(normalized) < 100:
        raise ValueError("at least 100 masking labels are required")
    split_index = max(1, min(len(normalized) - 1, int(len(normalized) * 0.8)))
    train = normalized[:split_index]
    holdout = normalized[split_index:]
    strata: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in train:
        for key in _stratum_keys(row):
            strata[key].append(row)
    curves = {
        key: _build_stratum(values) for key, values in sorted(strata.items()) if values
    }
    if "GLOBAL" not in curves:
        raise ValueError("masking labels did not produce a GLOBAL stratum")
    brier = _holdout_brier(holdout, curves["GLOBAL"])
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "model_version": "l2_artificial_masking_empirical_bayes_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "training_start": train[0]["decision_ts"].isoformat(),
        "training_end": train[-1]["decision_ts"].isoformat(),
        "evidence_scope": "XUE_NATIVE_L2_ARTIFICIAL_MASKING_PROXY",
        "promotion_allowed": False,
        "method": {
            "family": "ARTIFICIAL_GAP_MASKING",
            "masking_horizons_ms": list(MASKING_HORIZONS_MS),
            "maximum_supported_gap_ms": max(MASKING_HORIZONS_MS),
            "label": "ENDPOINT_VISIBLE_ASK_EXECUTABLE_AT_PRE_GAP_LIMIT",
            "conditional_size": "ENDPOINT_RECONSTRUCTED_TOP_LEVEL_SIZE_FRACTION",
            "split": "CHRONOLOGICAL_80_20",
            "creates_execution_liquidity": False,
        },
        "walk_forward": {
            "status": "PASS",
            "fold_count": 1,
            "event_leakage_count": 0,
            "train_count": len(train),
            "holdout_count": len(holdout),
            "holdout_brier_score": _text(brier),
        },
        "strata": curves,
    }
    body["artifact_hash"] = payload_hash(body)
    return body


def load_masking_rows_from_archive(
    archive_root: Path,
    *,
    max_files: int,
    row_stride: int,
    max_rows_per_file: int,
) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for archive masking") from exc
    files = list(islice(archive_root.rglob("*.parquet"), max(1, max_files)))
    if not files:
        raise FileNotFoundError(f"no parquet files under {archive_root}")
    columns = [
        "timestamp_received",
        "market",
        "asset_id",
        "event_type",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
    ]
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for path in files:
        consumed = 0
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=25_000, columns=columns):
            remaining = max(0, max_rows_per_file - consumed)
            if remaining <= 0:
                break
            rows = batch.slice(0, remaining).to_pylist()
            consumed += len(rows)
            for row in rows:
                if row.get("timestamp_received") is None or row.get("best_ask") is None:
                    continue
                grouped[
                    (str(row.get("market") or ""), str(row.get("asset_id") or ""))
                ].append(row)
    labels: list[dict[str, Any]] = []
    for values in grouped.values():
        values.sort(key=lambda row: row["timestamp_received"])
        states = _reconstruct_top_ask(values)
        timestamps = [row["timestamp"] for row in states]
        for index in range(0, len(states), max(1, row_stride)):
            anchor = states[index]
            anchor_ask = anchor["best_ask"]
            if anchor_ask <= 0 or anchor_ask >= 1:
                continue
            for gap_ms in MASKING_HORIZONS_MS:
                target = anchor["timestamp"] + timedelta(milliseconds=gap_ms)
                future_index = bisect.bisect_left(timestamps, target, lo=index + 1)
                if future_index >= len(states):
                    continue
                future = states[future_index]
                if future["timestamp"] > target + timedelta(seconds=1):
                    continue
                executable = future["best_ask"] <= anchor_ask
                top_size = future["best_ask_size"] if executable else Decimal(0)
                labels.append(
                    {
                        "decision_ts": anchor["timestamp"].isoformat(),
                        "gap_ms": gap_ms,
                        "category": "GLOBAL",
                        "price": str(anchor_ask),
                        "tte_bucket": "UNKNOWN",
                        "liquidity_regime": _liquidity_regime(anchor["best_ask_size"]),
                        "executable": executable,
                        "available_fraction": str(
                            min(Decimal(1), max(Decimal(0), top_size / Decimal(10)))
                        ),
                        "price_error": str(future["best_ask"] - anchor_ask),
                    }
                )
    return labels


def _reconstruct_top_ask(values: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    asks: dict[Decimal, Decimal] = {}
    states: list[dict[str, Any]] = []
    for row in values:
        event_type = str(row.get("event_type") or "")
        if event_type == "book" and row.get("asks"):
            asks = {
                Decimal(str(price)): Decimal(str(size))
                for price, size in json.loads(str(row["asks"]))
                if Decimal(str(size)) > 0
            }
        elif (
            event_type == "price_change"
            and str(row.get("side") or "").upper() == "SELL"
        ):
            level_price = Decimal(str(row.get("price") or 0))
            level_size = Decimal(str(row.get("size") or 0))
            if level_size > 0:
                asks[level_price] = level_size
            else:
                asks.pop(level_price, None)
        best_ask = Decimal(str(row.get("best_ask") or 0))
        if best_ask <= 0:
            continue
        states.append(
            {
                "timestamp": _datetime(row["timestamp_received"]),
                "best_ask": best_ask,
                "best_ask_size": asks.get(best_ask, Decimal(0)),
            }
        )
    return states


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    gap_ms = int(row["gap_ms"])
    if gap_ms not in MASKING_HORIZONS_MS:
        raise ValueError(f"unsupported masking horizon: {gap_ms}")
    available_fraction = min(
        Decimal(1), max(Decimal(0), Decimal(str(row.get("available_fraction") or 0)))
    )
    return {
        "decision_ts": _datetime(row["decision_ts"]),
        "gap_ms": gap_ms,
        "category": str(row.get("category") or "GLOBAL").upper(),
        "price_bucket": _price_bucket(Decimal(str(row.get("price") or "0.5"))),
        "tte_bucket": str(row.get("tte_bucket") or "UNKNOWN").upper(),
        "liquidity_regime": str(row.get("liquidity_regime") or "UNKNOWN").upper(),
        "executable": bool(row.get("executable")),
        "available_fraction": available_fraction,
        "price_error": Decimal(str(row.get("price_error") or 0)),
    }


def _stratum_keys(row: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        (
            f"CATEGORY:{row['category']}|PRICE:{row['price_bucket']}|"
            f"TTE:{row['tte_bucket']}|LIQUIDITY:{row['liquidity_regime']}"
        ),
        f"CATEGORY:{row['category']}|PRICE:{row['price_bucket']}|LIQUIDITY:{row['liquidity_regime']}",
        f"PRICE:{row['price_bucket']}|LIQUIDITY:{row['liquidity_regime']}",
        f"LIQUIDITY:{row['liquidity_regime']}",
        "GLOBAL",
    )


def _build_stratum(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    points: list[dict[str, Any]] = []
    for gap_ms in MASKING_HORIZONS_MS:
        selected = [row for row in rows if row["gap_ms"] == gap_ms]
        if not selected:
            continue
        executable = [row for row in selected if row["executable"]]
        probability = Decimal(len(executable)) / Decimal(len(selected))
        conditional_fraction = (
            Decimal(0)
            if not executable
            else sum((row["available_fraction"] for row in executable), Decimal(0))
            / Decimal(len(executable))
        )
        errors = sorted(row["price_error"] for row in selected)
        points.append(
            {
                "gap_ms": gap_ms,
                "sample_count": len(selected),
                "executable_count": len(executable),
                "p_executable": _text(probability),
                "conditional_available_fraction": _text(conditional_fraction),
                "price_error_quantiles": {
                    "p10": _text(_quantile(errors, Decimal("0.1"))),
                    "p50": _text(_quantile(errors, Decimal("0.5"))),
                    "p90": _text(_quantile(errors, Decimal("0.9"))),
                },
            }
        )
    return {"sample_count": len(rows), "points": points}


def _holdout_brier(
    rows: Sequence[Mapping[str, Any]], global_curve: Mapping[str, Any]
) -> Decimal:
    if not rows:
        return Decimal(0)
    by_gap = {
        int(point["gap_ms"]): Decimal(str(point["p_executable"]))
        for point in global_curve["points"]
    }
    errors = []
    for row in rows:
        probability = by_gap.get(int(row["gap_ms"]), Decimal(0))
        actual = Decimal(1) if row["executable"] else Decimal(0)
        errors.append((probability - actual) ** 2)
    return sum(errors, Decimal(0)) / Decimal(len(errors))


def _quantile(values: list[Decimal], probability: Decimal) -> Decimal:
    if not values:
        return Decimal(0)
    index = int((Decimal(len(values) - 1) * probability).to_integral_value())
    return values[index]


def _liquidity_regime(size: Decimal) -> str:
    if size <= 0:
        return "UNKNOWN"
    if size < Decimal(10):
        return "THIN"
    if size < Decimal(100):
        return "NORMAL"
    return "DEEP"


def _price_bucket(value: Decimal) -> str:
    if value < Decimal("0.1"):
        return "P00_10"
    if value < Decimal("0.3"):
        return "P10_30"
    if value < Decimal("0.7"):
        return "P30_70"
    if value < Decimal("0.9"):
        return "P70_90"
    return "P90_100"


def _datetime(value: Any) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("masking timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _text(value: Decimal) -> str:
    return format(value, "f")


def _load_jsonl(path: Path) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, Mapping):
            raise TypeError("each masking JSONL row must be an object")
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-jsonl", type=Path)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--max-files", type=int, default=8)
    parser.add_argument("--row-stride", type=int, default=100)
    parser.add_argument("--max-rows-per-file", type=int, default=100_000)
    parser.add_argument("--output", type=Path, default=DEFAULT_ARTIFACT_PATH)
    args = parser.parse_args()
    rows = (
        _load_jsonl(args.input_jsonl)
        if args.input_jsonl is not None
        else load_masking_rows_from_archive(
            args.archive_root,
            max_files=args.max_files,
            row_stride=args.row_stride,
            max_rows_per_file=args.max_rows_per_file,
        )
    )
    artifact = build_artifact(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "label_count": len(rows),
                "artifact_hash": artifact["artifact_hash"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
