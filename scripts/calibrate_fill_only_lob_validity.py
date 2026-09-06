#!/usr/bin/env python3
"""Calibrate fill-only validity rules against LOB holdout labels."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_only_lob_validity import (  # noqa: E402
    FillOnlyValidityRule,
    build_execution_stability_report,
    fill_only_validity_rule_grid,
    holdout_loss,
    save_lob_holdout_validity_rule,
)
from quant.backtest.fill_trade_validation import VALIDATION_OUT_DIR, run_lob_holdout_validation, write_report  # noqa: E402
from quant.backtest.fill_trade_validation import _summarize_holdout_comparisons  # noqa: E402


@dataclass(frozen=True)
class CalibrationCandidate:
    rule: FillOnlyValidityRule
    execution_params: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"rule": self.rule.as_dict(), "execution_params": self.execution_params}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--discover-market-limit", type=int, default=None)
    parser.add_argument("--build-tag", default="pmxt_v2_team_vs_team_main_lifecycle")
    parser.add_argument("--per-market", type=int, default=1)
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--book-ttl-ms", type=int, default=300_000)
    parser.add_argument("--depth-haircut", default="1.0")
    parser.add_argument("--price-tolerance", default="0.000001")
    parser.add_argument("--order-side", default="SAMPLE", choices=("SAMPLE", "BUY", "SELL"))
    parser.add_argument("--max-candidates", type=int, default=12)
    parser.add_argument("--split-key", default="market", choices=("market", "time", "sample"))
    parser.add_argument("--min-split-samples", type=int, default=100)
    parser.add_argument("--target-total-samples", type=int, default=1000)
    parser.add_argument("--output-profile", type=Path, default=PROJECT_ROOT / "config" / "execution" / "fill_only_lob_validity_profile.v1.json")
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "fill_only_lob_validity_calibration.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "fill_only_lob_validity_calibration.md")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows: list[dict[str, Any]] = []
    best: tuple[Any, CalibrationCandidate, dict[str, Any]] | None = None
    candidates = _calibration_candidate_grid()[: max(1, int(args.max_candidates))]
    previous_profile = os.environ.get("POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE")
    try:
        for index, candidate_cfg in enumerate(candidates):
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
                tmp_path = Path(handle.name)
            try:
                _write_candidate_profile(tmp_path, candidate_cfg)
                os.environ["POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE"] = str(tmp_path)
                payload = run_lob_holdout_validation(
                    market_slugs=args.market_slug or None,
                    build_tag=str(args.build_tag),
                    per_market=max(1, int(args.per_market)),
                    lookback_hours=max(1, int(args.lookback_hours)),
                    book_ttl_ms=max(1, int(args.book_ttl_ms)),
                    depth_haircut=args.depth_haircut,
                    price_tolerance=args.price_tolerance,
                    order_side=args.order_side,
                    fill_only_profile="lob_holdout_calibrated_fill_only",
                    discover_market_limit=args.discover_market_limit,
                )
            finally:
                tmp_path.unlink(missing_ok=True)
            summary = dict(payload.get("summary") or {})
            verdict_counts = dict(summary.get("verdict_counts") or {})
            split_metrics = _split_holdout_metrics(summary.get("comparisons") or [], split_key=args.split_key)
            validation_metrics = _nonempty_split(split_metrics, "validation") or summary
            loss = holdout_loss(validation_metrics, samples=int(validation_metrics.get("samples") or summary.get("samples") or 0))
            candidate = {
                "index": index,
                "loss": loss,
                "status": payload.get("status"),
                "candidate": candidate_cfg.as_dict(),
                "rule": candidate_cfg.rule.as_dict(),
                "execution_params": candidate_cfg.execution_params,
                "samples": int(summary.get("samples") or 0),
                "verdict_counts": verdict_counts,
                "false_positive_rate": summary.get("false_positive_rate"),
                "false_negative_rate": summary.get("false_negative_rate"),
                "precision": summary.get("precision"),
                "recall": summary.get("recall"),
                "avg_abs_size_error": summary.get("avg_abs_size_error"),
                "avg_abs_price_error": summary.get("avg_abs_price_error"),
                "overfill_rate": summary.get("overfill_rate"),
                "underfill_rate": summary.get("underfill_rate"),
                "adverse_price_error": summary.get("adverse_price_error"),
                "both_filled_same_pct": summary.get("both_filled_same_pct"),
                "split_metrics": split_metrics,
            }
            rows.append(candidate)
            key = (loss, -int(verdict_counts.get("both_filled_same", 0) or 0), index)
            if best is None or key < best[0]:
                best = (key, candidate_cfg, payload)
    finally:
        if previous_profile is None:
            os.environ.pop("POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE", None)
        else:
            os.environ["POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE"] = previous_profile

    if best is None:
        report = {"title": "Fill-Only LOB Validity Calibration", "status": "fail", "summary": {"reason": "no candidates"}}
        write_report(report, output_json=args.output_json, output_md=args.output_md)
        print(f"status=fail json={args.output_json} md={args.output_md}")
        return 1

    _, best_candidate, best_payload = best
    best_rule = best_candidate.rule
    best_summary = dict(best_payload.get("summary") or {})
    best_counts = dict(best_summary.get("verdict_counts") or {})
    best_split_metrics = _split_holdout_metrics(best_summary.get("comparisons") or [], split_key=args.split_key)
    stability_report = build_execution_stability_report(
        best_split_metrics,
        min_split_samples=max(1, int(args.min_split_samples)),
        target_total_samples=max(1, int(args.target_total_samples)),
    )
    sample_count = int(best_summary.get("samples") or 0)
    best_selection_metrics = _nonempty_split(best_split_metrics, "validation") or best_summary
    best_loss = holdout_loss(best_selection_metrics, samples=int(best_selection_metrics.get("samples") or sample_count))
    report = {
        "title": "Fill-Only LOB Validity Calibration",
        "status": "pass" if sample_count > 0 else "review",
        "summary": {
            "selected_rule": best_rule.as_dict(),
            "selected_execution_params": best_candidate.execution_params,
            "selected_loss": best_loss,
            "samples": sample_count,
            "verdict_counts": best_counts,
            "false_positive_rate": best_summary.get("false_positive_rate"),
            "false_negative_rate": best_summary.get("false_negative_rate"),
            "precision": best_summary.get("precision"),
            "recall": best_summary.get("recall"),
            "avg_abs_size_error": best_summary.get("avg_abs_size_error"),
            "avg_abs_price_error": best_summary.get("avg_abs_price_error"),
            "overfill_rate": best_summary.get("overfill_rate"),
            "underfill_rate": best_summary.get("underfill_rate"),
            "adverse_price_error": best_summary.get("adverse_price_error"),
            "split_metrics": best_split_metrics,
            "split_key": args.split_key,
            "execution_stability": stability_report,
            "candidates_evaluated": len(rows),
            "output_profile": str(args.output_profile),
        },
        "candidates": sorted(rows, key=lambda row: (row["loss"], row["index"])),
    }
    _write_formal_profile(args.output_profile, best_candidate, best_summary, best_split_metrics, stability_report=stability_report, split_key=args.split_key, report=report)
    write_report(report, output_json=args.output_json, output_md=args.output_md)
    print(f"status={report['status']} profile={args.output_profile} json={args.output_json} md={args.output_md}")
    return 0 if report["status"] == "pass" else 1

def _split_holdout_metrics(comparisons: Sequence[Mapping[str, Any]], *, split_key: str = "market") -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[Mapping[str, Any]]] = {"calibration": [], "validation": [], "holdout": []}
    for row in comparisons:
        key = _split_key_for_row(row, split_key=split_key)
        digest = int(hashlib.sha1(key.encode("utf-8")).hexdigest()[:8], 16) % 10
        bucket = "calibration" if digest < 6 else "validation" if digest < 8 else "holdout"
        buckets[bucket].append(row)
    return {name: _summarize_holdout_comparisons(rows) for name, rows in buckets.items()}


def _calibration_candidate_grid() -> list[CalibrationCandidate]:
    rows: list[CalibrationCandidate] = []
    execution_grid = [
        {"participation_rate": "0.005", "price_buffer_abs": "0.005", "trailing_volume_cap_fraction": "0.005", "market_window_cap_fraction": "0.50"},
        {"participation_rate": "0.010", "price_buffer_abs": "0.005", "trailing_volume_cap_fraction": "0.010", "market_window_cap_fraction": "0.75"},
        {"participation_rate": "0.010", "price_buffer_abs": "0.010", "trailing_volume_cap_fraction": "0.010", "market_window_cap_fraction": "0.75"},
        {"participation_rate": "0.025", "price_buffer_abs": "0.010", "trailing_volume_cap_fraction": "0.025", "market_window_cap_fraction": "1.00"},
    ]
    for rule in fill_only_validity_rule_grid():
        for execution_params in execution_grid:
            rows.append(CalibrationCandidate(rule=rule, execution_params={**_default_execution_params(rule), **execution_params}))
    return rows


def _default_execution_params(rule: FillOnlyValidityRule) -> dict[str, Any]:
    return {
        "name": rule.name,
        "execution_horizon_by_tif": {"FOK": "1 block", "IOC": "1 block / 1s", "FAK": "1 block / 1s", "GTC": "30s", "GTD": "expires_at"},
        "quote_proxy_ttl_seconds": str(rule.trailing_window_seconds),
        "market_window_blocks": 20,
        "p_depth_valid_threshold": str(rule.min_probability),
    }


def _write_candidate_profile(path: Path, candidate: CalibrationCandidate) -> None:
    payload = {
        "schema_version": "fill_only_lob_validity_profile_v1",
        "profile_name": "lob_holdout_calibrated_fill_only_candidate",
        "profile_role": "calibration_candidate",
        "trained_on": "candidate_grid",
        "params": {**candidate.rule.as_dict(), **candidate.execution_params},
        "boundary": {
            "runtime_lob_usage": "none",
            "calibration_lob_usage": "offline_label_only",
            "default_result_policy": "reject_uncertain_orders",
            "not_l2_l3_or_queue_accurate": True,
        },
    }
    path.write_text(json_dumps(payload), encoding="utf-8")


def _split_key_for_row(row: Mapping[str, Any], *, split_key: str) -> str:
    sample = row.get("sample") if isinstance(row.get("sample"), Mapping) else {}
    if split_key == "market":
        return "|".join(str(sample.get(part) or "") for part in ("market_id", "market_slug"))
    if split_key == "time":
        fill_time = str(sample.get("fill_time") or sample.get("block_time") or "")
        return fill_time[:10] or "|".join(str(sample.get(part) or "") for part in ("market_id", "sample_id"))
    return "|".join(str(sample.get(part) or "") for part in ("market_id", "market_slug", "sample_id", "tx_hash", "log_index"))


def _nonempty_split(split_metrics: Mapping[str, Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    row = split_metrics.get(name)
    if row and int(row.get("samples") or 0) > 0:
        return row
    return None


def _write_formal_profile(
    path: Path,
    candidate: CalibrationCandidate,
    summary: Mapping[str, Any],
    split_metrics: Mapping[str, Any],
    *,
    stability_report: Mapping[str, Any],
    split_key: str,
    report: Mapping[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rule = candidate.rule
    payload = {
        "schema_version": "fill_only_lob_validity_profile_v1",
        "profile_name": "lob_holdout_calibrated_fill_only_v1",
        "profile_role": "primary_conservative",
        "trained_on": datetime.now(timezone.utc).isoformat(),
        "holdout_set": str(summary.get("build_tag") or "lob_holdout_validation"),
        "params": {
            **rule.as_dict(),
            **candidate.execution_params,
            "price_buffer_ticks": "1",
            "execution_horizon_by_tif": {"FOK": "1 block", "IOC": "1 block / 1s", "FAK": "1 block / 1s", "GTC": "30s", "GTD": "expires_at"},
            "quote_proxy_ttl_seconds": str(rule.trailing_window_seconds),
            "p_depth_valid_threshold": str(rule.min_probability),
        },
        "holdout_metrics": {
            key: summary.get(key)
            for key in ("samples", "precision", "recall", "false_positive_rate", "false_negative_rate", "overfill_rate", "underfill_rate", "adverse_price_error", "avg_abs_price_error", "avg_abs_size_error")
        },
        "split_key": split_key,
        "split_metrics": split_metrics,
        "execution_stability": stability_report,
        "boundary": {
            "runtime_lob_usage": "none",
            "calibration_lob_usage": "offline_label_only",
            "default_result_policy": "reject_uncertain_orders",
            "not_l2_l3_or_queue_accurate": True,
        },
        "calibration_report": report,
    }
    path.write_text(json_dumps(payload), encoding="utf-8")


def json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
