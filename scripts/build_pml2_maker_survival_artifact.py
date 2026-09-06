#!/usr/bin/env python3
"""Build the versioned PML2 Maker survival artifact from frozen evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.pml2.survival import (
    DEFAULT_ARTIFACT_PATH,
    SCHEMA_VERSION,
)

DEFAULT_OFFLINE = ROOT / "runtime_outputs/maker_calibration/offline-summary-current.json"
DEFAULT_AUTHENTICATED = ROOT / "reports/maker/holdout-current/evaluation.json"
DEFAULT_AUTHENTICATED_ROWS = (
    ROOT
    / "runtime_outputs/maker_calibration/authenticated-current/authenticated_holdout.jsonl"
)


def build_artifact(
    *,
    offline_path: Path,
    authenticated_path: Path,
    authenticated_rows_path: Path,
) -> dict[str, Any]:
    offline = _object(json.loads(offline_path.read_text(encoding="utf-8")))
    authenticated = _object(
        json.loads(authenticated_path.read_text(encoding="utf-8"))
    )
    if offline.get("status") != "PASS_OFFLINE":
        raise ValueError("offline Maker report is not PASS_OFFLINE")
    split = _object(offline.get("split_manifest"))
    if split.get("status") != "PASS" or int(split.get("event_leakage_count") or 0):
        raise ValueError("offline Maker split is not leakage-free")
    maker = _object(offline.get("maker"))
    walk_forward = _object(maker.get("walk_forward"))
    if walk_forward.get("status") != "PASS":
        raise ValueError("Maker walk-forward evidence is not PASS")
    aggregate = _object(walk_forward.get("aggregate"))
    rows = aggregate.get("rows")
    samples = offline.get("samples")
    if not isinstance(rows, list) or not isinstance(samples, list):
        raise TypeError("Maker report is missing walk-forward rows or samples")
    trial_strata: dict[str, dict[str, str]] = {}
    for sample in samples:
        if not isinstance(sample, Mapping):
            continue
        stratum = _object(sample.get("calibration_stratum"))
        trial_id = str(stratum.get("independent_trial_id") or "")
        if not trial_id:
            order = _object(sample.get("maker_order"))
            trial_id = str(order.get("trial_id") or "")
        if not trial_id:
            continue
        candidate = _object(sample.get("candidate"))
        trial_strata[trial_id] = {
            "category": str(
                stratum.get("category") or candidate.get("category") or "GLOBAL"
            ).upper(),
            "side": str(stratum.get("side") or "UNKNOWN").upper(),
            "position": str(
                stratum.get("quote_position") or "AT_BEST"
            ).upper(),
            "queue": str(stratum.get("queue_bucket") or "UNKNOWN").upper(),
        }
    trial_rows: dict[str, list[tuple[int, str, float]]] = defaultdict(list)
    fallback_strata: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        trial_id = str(row.get("trial_id") or "")
        if not trial_id:
            continue
        stratum = trial_strata.get(
            trial_id,
            {
                "category": "GLOBAL",
                "side": str(row.get("side") or "UNKNOWN").upper(),
                "position": str(row.get("quote_position") or "AT_BEST").upper(),
                "queue": "UNKNOWN",
            },
        )
        horizon = int(float(str(row.get("horizon_seconds") or 0)))
        if horizon <= 0:
            continue
        trial_strata.setdefault(trial_id, stratum)
        fallback_strata[trial_id] = stratum
        label = _object(row.get("label"))
        filled_lower = float(str(label.get("filled_size_lower") or 0))
        filled_upper = float(str(label.get("filled_size_upper") or 0))
        fill_fraction = (
            0.0
            if filled_upper <= 0
            else min(1.0, max(0.0, filled_lower / filled_upper))
        )
        trial_rows[trial_id].append(
            (horizon, str(row.get("label_class") or "").upper(), fill_fraction)
        )
    strata_trials: dict[str, list[list[tuple[int, str, float]]]] = defaultdict(list)
    for trial_id, observations in trial_rows.items():
        stratum = trial_strata.get(trial_id, fallback_strata[trial_id])
        for key in _stratum_keys(stratum):
            strata_trials[key].append(observations)
    curves: dict[str, dict[str, Any]] = {}
    for key, observations_by_trial in sorted(strata_trials.items()):
        points: list[dict[str, Any]] = []
        monotonic_fill = 0.0
        total_samples = 0
        total_events = 0
        horizons = sorted(
            {
                horizon
                for observations in observations_by_trial
                for horizon, _, _ in observations
            }
        )
        for horizon in horizons:
            sample_count = 0
            event_count = 0
            censored_count = 0
            conditional_fractions: list[float] = []
            for observations in observations_by_trial:
                confirmed = [
                    (observed_horizon, fill_fraction)
                    for observed_horizon, label, fill_fraction in observations
                    if label == "STRICT_CONFIRMED_FILL"
                ]
                known = [
                    observed_horizon
                    for observed_horizon, label, _ in observations
                    if label
                    in {"STRICT_CONFIRMED_FILL", "OBSERVED_TAPE_NO_FILL"}
                ]
                first_fill = min(confirmed, default=None)
                last_known = max(known) if known else 0
                if first_fill is not None and first_fill[0] <= horizon:
                    sample_count += 1
                    event_count += 1
                    conditional_fractions.append(first_fill[1])
                elif last_known >= horizon:
                    sample_count += 1
                else:
                    censored_count += 1
            if sample_count <= 0:
                continue
            observed = event_count / sample_count
            monotonic_fill = max(monotonic_fill, observed)
            total_samples = max(total_samples, sample_count)
            total_events = max(total_events, event_count)
            points.append(
                {
                    "horizon_seconds": horizon,
                    "sample_count": sample_count,
                    "event_count": event_count,
                    "censored_count": censored_count,
                    "fill_probability": _decimal_text(monotonic_fill),
                    "survival_probability": _decimal_text(1.0 - monotonic_fill),
                    "conditional_fill_fraction": _decimal_text(
                        sum(conditional_fractions) / len(conditional_fractions)
                        if conditional_fractions
                        else 0.0
                    ),
                }
            )
        if points:
            curves[key] = {
                "sample_count": total_samples,
                "event_count": total_events,
                "points": points,
            }
    if "GLOBAL" not in curves:
        raise ValueError("could not build a global Maker survival curve")
    dates = [
        datetime.fromisoformat(str(value)).date()
        for value in split.get("utc_dates", [])
    ]
    if not dates:
        raise ValueError("Maker split manifest has no UTC dates")
    authenticated_rows_hash = _file_hash(authenticated_rows_path)
    generated_candidates = [
        datetime.fromisoformat(str(value))
        for value in (
            offline.get("generated_at"),
            authenticated.get("generated_at"),
        )
        if value
    ]
    generated_at = (
        max(generated_candidates)
        if generated_candidates
        else datetime.combine(max(dates), time.max, tzinfo=timezone.utc)
    )
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "model_version": "interval_censored_discrete_survival_walk_forward_v1",
        "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
        "training_start": datetime.combine(
            min(dates), time.min, tzinfo=timezone.utc
        ).isoformat(),
        "training_end": datetime.combine(
            max(dates), time.max, tzinfo=timezone.utc
        ).isoformat(),
        "evidence_scope": "ORDERFILLED_PROXY_PLUS_AUTHENTICATED_OWN_ORDER_HOLDOUT",
        "method": {
            "family": "INTERVAL_CENSORED_DISCRETE_SURVIVAL",
            "right_censoring": True,
            "independent_unit": "MAKER_TRIAL_NOT_HORIZON_ROW",
            "curve_rule": "MONOTONIC_CUMULATIVE_INCIDENCE_BY_HORIZON",
            "stratum_fallback": [
                "CATEGORY_SIDE_POSITION_QUEUE",
                "CATEGORY_SIDE_POSITION",
                "SIDE_POSITION",
                "GLOBAL",
            ],
            "minimum_runtime_stratum_samples": 20,
            "hierarchical_prior_strength": 50,
            "creates_execution_liquidity": False,
        },
        "walk_forward": {
            "status": "PASS",
            "fold_count": int(walk_forward.get("fold_count") or 0),
            "event_leakage_count": int(split.get("event_leakage_count") or 0),
            "final_holdout_start": walk_forward.get("final_holdout_start"),
            "independent_trial_count": int(
                aggregate.get("independent_trial_count") or 0
            ),
            "source_report_sha256": _file_hash(offline_path),
        },
        "authenticated_holdout": {
            "status": authenticated.get("status"),
            "sample_count": int(authenticated.get("sample_count") or 0),
            "outcome_counts": dict(authenticated.get("outcome_counts") or {}),
            "promotion_allowed": bool(authenticated.get("promotion_allowed")),
            "calibration_conclusion": authenticated.get(
                "calibration_conclusion"
            ),
            "source_evaluation_sha256": _file_hash(authenticated_path),
            "source_rows_sha256": authenticated_rows_hash,
            "source_rows_available": authenticated_rows_path.stat().st_size > 0,
        },
        "curves": curves,
    }
    body["artifact_hash"] = _payload_hash(body)
    return body


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", type=Path, default=DEFAULT_OFFLINE)
    parser.add_argument("--authenticated", type=Path, default=DEFAULT_AUTHENTICATED)
    parser.add_argument(
        "--authenticated-rows",
        type=Path,
        default=DEFAULT_AUTHENTICATED_ROWS,
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_ARTIFACT_PATH)
    args = parser.parse_args()
    artifact = build_artifact(
        offline_path=args.offline,
        authenticated_path=args.authenticated,
        authenticated_rows_path=args.authenticated_rows,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "artifact_hash": artifact["artifact_hash"],
                "curve_count": len(artifact["curves"]),
                "authenticated_trial_count": artifact["authenticated_holdout"][
                    "sample_count"
                ],
                "authenticated_promotion_allowed": artifact[
                    "authenticated_holdout"
                ]["promotion_allowed"],
            },
            sort_keys=True,
        )
    )
    return 0


def _stratum_keys(stratum: Mapping[str, str]) -> tuple[str, ...]:
    category = stratum["category"]
    side = stratum["side"]
    position = stratum["position"]
    queue = stratum["queue"]
    return (
        "GLOBAL",
        f"SIDE:{side}|POSITION:{position}",
        f"CATEGORY:{category}|SIDE:{side}|POSITION:{position}",
        f"CATEGORY:{category}|SIDE:{side}|POSITION:{position}|QUEUE:{queue}",
    )


def _object(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _decimal_text(value: float) -> str:
    return format(value, ".12f").rstrip("0").rstrip(".") or "0"


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("artifact_hash", None)
    return hashlib.sha256(
        json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
