"""Analyze Registry soak JSONL output and explain failures.

This module is intentionally read-only. It consumes the samples emitted by
``quant.market.registry_soak_monitor`` and turns them into a compact health
report that can be used by CLI, dashboard jobs, or CI artifacts.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class SoakFailureWindow:
    start_sample: int
    end_sample: int
    length: int
    start_at: str | None = None
    end_at: str | None = None
    reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class SoakReport:
    status: str
    total_samples: int
    pass_samples: int
    fail_samples: int
    warn_samples: int
    failure_threshold: int
    max_consecutive_failures: int
    first_sample_at: str | None = None
    last_sample_at: str | None = None
    first_fail_at: str | None = None
    last_fail_at: str | None = None
    reason_counts: dict[str, int] = field(default_factory=dict)
    ws_unhealthy_samples: int = 0
    pending_outbox_stale_samples: int = 0
    max_pending_outbox: int = 0
    max_live_to_discovered_10m: int = 0
    max_execution_universe_count: int = 0
    latest_execution_universe_count: int | None = None
    latest_pending_outbox: int | None = None
    latest_ws_healthy: bool | None = None
    failure_windows: list[SoakFailureWindow] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_soak_samples(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_no}: {exc}") from exc
            if isinstance(row, dict):
                rows.append(row)
    return rows


def build_soak_report(samples: Iterable[Mapping[str, Any]], *, failure_threshold: int = 3) -> SoakReport:
    rows = [dict(sample) for sample in samples]
    threshold = max(1, int(failure_threshold))
    if not rows:
        return SoakReport(
            status="WARN",
            total_samples=0,
            pass_samples=0,
            fail_samples=0,
            warn_samples=0,
            failure_threshold=threshold,
            max_consecutive_failures=0,
            recommendations=["No soak samples were found; run registry_soak_monitor first."],
        )

    status_counts = Counter(str(row.get("status") or "UNKNOWN").upper() for row in rows)
    reason_counts: Counter[str] = Counter()
    ws_unhealthy = 0
    pending_stale = 0
    max_pending = 0
    max_regressions = 0
    max_execution = 0
    first_fail_at: str | None = None
    last_fail_at: str | None = None
    consecutive = 0
    max_consecutive = 0
    open_window_start: int | None = None
    open_window_reasons: Counter[str] = Counter()
    windows: list[SoakFailureWindow] = []

    for idx, row in enumerate(rows):
        row_status = str(row.get("status") or "UNKNOWN").upper()
        reasons = [str(reason) for reason in (row.get("reasons") or []) if str(reason)]
        reason_counts.update(reasons)
        if row.get("ws_healthy") is False:
            ws_unhealthy += 1
        if bool(row.get("pending_outbox_stale")):
            pending_stale += 1
        max_pending = max(max_pending, _int(row.get("pending_outbox")))
        max_regressions = max(max_regressions, _int(row.get("live_to_discovered_10m")))
        latest_health = row.get("latest_health") if isinstance(row.get("latest_health"), dict) else {}
        max_execution = max(max_execution, _int(latest_health.get("execution_universe_count")))

        if row_status == "FAIL":
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
            if first_fail_at is None:
                first_fail_at = _sample_time(row)
            last_fail_at = _sample_time(row)
            if open_window_start is None:
                open_window_start = idx
                open_window_reasons = Counter()
            open_window_reasons.update(reasons or ["unknown"])
        else:
            if open_window_start is not None:
                windows.append(_window(rows, open_window_start, idx - 1, open_window_reasons))
                open_window_start = None
                open_window_reasons = Counter()
            consecutive = 0

    if open_window_start is not None:
        windows.append(_window(rows, open_window_start, len(rows) - 1, open_window_reasons))

    latest = rows[-1]
    latest_health = latest.get("latest_health") if isinstance(latest.get("latest_health"), dict) else {}
    status = "PASS"
    if max_consecutive >= threshold:
        status = "FAIL"
    elif status_counts["FAIL"] > 0 or status_counts["WARN"] > 0:
        status = "WARN"

    recommendations = _recommendations(
        status=status,
        reason_counts=reason_counts,
        ws_unhealthy_samples=ws_unhealthy,
        pending_outbox_stale_samples=pending_stale,
        max_live_to_discovered_10m=max_regressions,
        max_execution_universe_count=max_execution,
        latest_execution_universe_count=_optional_int(latest_health.get("execution_universe_count")),
    )
    return SoakReport(
        status=status,
        total_samples=len(rows),
        pass_samples=status_counts["PASS"],
        fail_samples=status_counts["FAIL"],
        warn_samples=status_counts["WARN"],
        failure_threshold=threshold,
        max_consecutive_failures=max_consecutive,
        first_sample_at=_sample_time(rows[0]),
        last_sample_at=_sample_time(latest),
        first_fail_at=first_fail_at,
        last_fail_at=last_fail_at,
        reason_counts=dict(sorted(reason_counts.items())),
        ws_unhealthy_samples=ws_unhealthy,
        pending_outbox_stale_samples=pending_stale,
        max_pending_outbox=max_pending,
        max_live_to_discovered_10m=max_regressions,
        max_execution_universe_count=max_execution,
        latest_execution_universe_count=_optional_int(latest_health.get("execution_universe_count")),
        latest_pending_outbox=_optional_int(latest.get("pending_outbox")),
        latest_ws_healthy=latest.get("ws_healthy") if isinstance(latest.get("ws_healthy"), bool) else None,
        failure_windows=windows,
        recommendations=recommendations,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", required=True, type=Path, help="Path emitted by registry_soak_monitor --jsonl-out.")
    parser.add_argument("--failure-threshold", type=int, default=3)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args(argv)
    report = build_soak_report(load_soak_samples(args.jsonl), failure_threshold=args.failure_threshold)
    payload = report.as_dict()
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    print(text)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(text + "\n", encoding="utf-8")
    return 0 if report.status != "FAIL" else 1


def _window(rows: list[dict[str, Any]], start: int, end: int, reasons: Counter[str]) -> SoakFailureWindow:
    return SoakFailureWindow(
        start_sample=start,
        end_sample=end,
        length=end - start + 1,
        start_at=_sample_time(rows[start]),
        end_at=_sample_time(rows[end]),
        reasons=dict(sorted(reasons.items())),
    )


def _sample_time(row: Mapping[str, Any]) -> str | None:
    for key in ("sampled_at", "recorded_at", "created_at"):
        value = row.get(key)
        if value:
            return str(value)
    return None


def _int(value: Any) -> int:
    parsed = _optional_int(value)
    return parsed if parsed is not None else 0


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _recommendations(
    *,
    status: str,
    reason_counts: Counter[str],
    ws_unhealthy_samples: int,
    pending_outbox_stale_samples: int,
    max_live_to_discovered_10m: int,
    max_execution_universe_count: int,
    latest_execution_universe_count: int | None,
) -> list[str]:
    recommendations: list[str] = []
    if ws_unhealthy_samples:
        recommendations.append("WS lifecycle was unhealthy in at least one sample; inspect registry WS reconnect/proxy logs.")
    if pending_outbox_stale_samples or reason_counts.get("pending_outbox"):
        recommendations.append("Pending outbox became stale; inspect the outbox publisher or LOB consumer loop.")
    if max_live_to_discovered_10m or reason_counts.get("live_to_discovered_regression"):
        recommendations.append("LIVE->DISCOVERED regressions appeared; audit placeholder/status_missing handling.")
    if max_execution_universe_count == 0:
        recommendations.append("Execution universe never became non-empty; verify CLOB book probe and BookState readiness.")
    elif latest_execution_universe_count == 0:
        recommendations.append("Execution universe was non-empty but ended empty; inspect close/resolution and stale transitions.")
    if status == "PASS" and not recommendations:
        recommendations.append("No sustained soak failures detected.")
    return recommendations


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
