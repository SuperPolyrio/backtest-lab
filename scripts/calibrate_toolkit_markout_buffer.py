#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
Q = Decimal("0.0000000001")
CONTEXT_FIELDS = ("category", "league", "price_bucket", "liquidity_regime")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, action="append", default=[])
    parser.add_argument("--tau", type=int, default=30)
    parser.add_argument("--min-samples", type=int, default=100)
    parser.add_argument("--min-markets", type=int, default=10)
    parser.add_argument("--min-observations", type=int, default=3)
    parser.add_argument("--min-coverage", type=Decimal, default=Decimal("0.80"))
    parser.add_argument(
        "--min-observation-coverage", type=Decimal, default=Decimal("0.50")
    )
    parser.add_argument(
        "--max-observation-sample-share", type=Decimal, default=Decimal("0.50")
    )
    parser.add_argument("--max-buffer", type=Decimal, default=Decimal("0.02"))
    parser.add_argument("--fallback-buffer", type=Decimal, default=Decimal("0.005"))
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "config/execution/trade_only_price_buffer.v1.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    observations = []
    rejected_inputs = []
    for path in args.input:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rejection = _rejection_reason(payload, tau=args.tau)
        if rejection is not None:
            rejected_inputs.append({"source": str(path), "reason": rejection})
            continue
        row = _observation(payload, tau=args.tau, source=str(path))
        if row is not None:
            observations.append(row)
    total_samples = sum(row["samples"] for row in observations)
    total_markets = sum(row["markets"] for row in observations)
    coverage = _coverage(observations)
    observations_meeting_coverage, max_observation_sample_share = (
        _observation_quality(observations, args.min_observation_coverage)
    )
    ready = (
        total_samples >= args.min_samples
        and total_markets >= args.min_markets
        and len(observations) >= args.min_observations
        and coverage >= args.min_coverage
        and observations_meeting_coverage >= args.min_observations
        and max_observation_sample_share <= args.max_observation_sample_share
    )
    status = "READY" if ready else "INSUFFICIENT_SAMPLE"
    global_buffer = (
        _buffer(observations, args.max_buffer)
        if observations
        else args.fallback_buffer
    )
    levels = []
    for fields in (("category",), ("category", "league"), CONTEXT_FIELDS):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in observations:
            if all(row["context"].get(field) for field in fields):
                grouped["|".join(row["context"][field] for field in fields)].append(row)
        cells = {
            key: {
                "buffer": str(_buffer(rows, args.max_buffer).quantize(Q)),
                "samples": sum(row["samples"] for row in rows),
                "requested_samples": sum(row["requested_samples"] for row in rows),
                "coverage": str(_coverage(rows).quantize(Q)),
            }
            for key, rows in grouped.items()
            if sum(row["samples"] for row in rows) >= args.min_samples
        }
        levels.append({"fields": list(fields), "cells": cells})
    output = {
        "schema_version": "trade_only_price_buffer_v1",
        "status": status,
        "source": "polymarket_toolkit_excess_markout",
        "tau_seconds": args.tau,
        "activation_requirements": {
            "min_samples": args.min_samples,
            "min_markets": args.min_markets,
            "min_observations": args.min_observations,
            "min_coverage": str(args.min_coverage),
            "min_observation_coverage": str(args.min_observation_coverage),
            "max_observation_sample_share": str(args.max_observation_sample_share),
        },
        "global": {
            "buffer": str(global_buffer.quantize(Q)),
            "samples": total_samples,
            "requested_samples": sum(row["requested_samples"] for row in observations),
            "markets": total_markets,
            "coverage": str(coverage.quantize(Q)),
            "observations_meeting_coverage": observations_meeting_coverage,
            "max_observation_sample_share": str(
                max_observation_sample_share.quantize(Q)
            ),
        },
        "levels": levels,
        "observations": _json_ready(observations),
        "rejected_inputs": rejected_inputs,
        "fallback_buffer": str(args.fallback_buffer),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": status, "output": str(args.output), "samples": total_samples}))
    return 0


def _observation(payload: dict[str, Any], *, tau: int, source: str) -> dict[str, Any] | None:
    if _rejection_reason(payload, tau=tau) is not None:
        return None
    result = next(
        (
            row
            for row in payload.get("results", [])
            if isinstance(row, dict) and int(row.get("tau", -1)) == tau
        ),
        None,
    )
    if not isinstance(result, dict) or result.get("excessCents") is None:
        return None
    raw_mine = result.get("mine")
    mine: dict[str, Any] = raw_mine if isinstance(raw_mine, dict) else {}
    raw_context = payload.get("context")
    context: dict[str, Any] = raw_context if isinstance(raw_context, dict) else {}
    half_window = int(payload["vwapHalfWindowSec"])
    return {
        "source": source,
        "samples": int(mine.get("n") or 0),
        "requested_samples": int(payload.get("fills") or 0),
        "markets": int(payload.get("markets") or 0),
        "coverage": Decimal(str(result.get("coverage") or 0)),
        "excess_cents": Decimal(str(result["excessCents"])),
        "reference_window": {
            "start_seconds_after_fill": tau - half_window,
            "end_seconds_after_fill": tau + half_window,
        },
        "context": {field: str(context.get(field) or "") for field in CONTEXT_FIELDS},
    }


def _rejection_reason(payload: dict[str, Any], *, tau: int) -> str | None:
    raw_half_window = payload.get("vwapHalfWindowSec")
    if not isinstance(raw_half_window, (int, float)) or raw_half_window <= 0:
        return "REFERENCE_WINDOW_UNKNOWN"
    if raw_half_window >= tau:
        return "REFERENCE_WINDOW_NOT_STRICTLY_POST_FILL"
    return None


def _buffer(rows: list[dict[str, Any]], maximum: Decimal) -> Decimal:
    if not rows:
        return Decimal(0)
    weighted_excess = sum(
        (row["excess_cents"] * row["samples"] for row in rows), Decimal(0)
    ) / Decimal(max(1, sum(row["samples"] for row in rows)))
    return max(Decimal(0), min(maximum, -weighted_excess / Decimal(100)))


def _coverage(rows: list[dict[str, Any]]) -> Decimal:
    requested = sum(int(row["requested_samples"]) for row in rows)
    if requested <= 0:
        return Decimal(0)
    covered = sum(int(row["samples"]) for row in rows)
    return Decimal(covered) / Decimal(requested)


def _observation_quality(
    rows: list[dict[str, Any]], min_coverage: Decimal
) -> tuple[int, Decimal]:
    qualifying = sum(row["coverage"] >= min_coverage for row in rows)
    total_samples = sum(int(row["samples"]) for row in rows)
    largest_share = (
        Decimal(max(int(row["samples"]) for row in rows)) / Decimal(total_samples)
        if total_samples > 0
        else Decimal(0)
    )
    return qualifying, largest_share


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
