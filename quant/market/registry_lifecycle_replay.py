"""Replay and audit Market Registry lifecycle events.

The replay operates on exported rows and is read-only. It is meant for audit
tools and tests that need to prove dynamic add/remove behavior can be replayed
without touching the live registry tables.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


STATE_RANK = {
    "DISCOVERED": 0,
    "TRADABLE_PENDING_BOOK": 1,
    "LIVE": 2,
    "STALE": 2,
    "CLOSING": 3,
    "RESOLVED": 4,
    "ARCHIVED": 5,
}
TERMINAL_STATES = {"RESOLVED", "ARCHIVED"}


@dataclass(frozen=True)
class LifecycleReplayIssue:
    asset_id: str
    issue_type: str
    event_index: int
    old_state: str | None = None
    new_state: str | None = None
    event_type: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class LifecycleReplayReport:
    status: str
    events_replayed: int
    assets_seen: int
    final_states: dict[str, str]
    issue_counts: dict[str, int]
    issues: list[LifecycleReplayIssue] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def replay_lifecycle_events(events: Iterable[Mapping[str, Any]], *, max_issues: int = 100) -> LifecycleReplayReport:
    rows = [dict(row) for row in events]
    final_states: dict[str, str] = {}
    issue_counts: defaultdict[str, int] = defaultdict(int)
    issues: list[LifecycleReplayIssue] = []

    for idx, row in enumerate(rows):
        asset_id = str(row.get("asset_id") or row.get("token_id") or "").strip()
        if not asset_id:
            _record_issue(issues, issue_counts, "missing_asset_id", idx, row, max_issues=max_issues)
            continue
        old_state = _state(row.get("old_state"))
        new_state = _state(row.get("new_state") or row.get("market_state"))
        event_type = str(row.get("event_type") or "").strip() or None
        previous = final_states.get(asset_id)

        if old_state and previous and old_state != previous:
            _record_issue(
                issues,
                issue_counts,
                "old_state_mismatch",
                idx,
                row,
                detail=f"expected old_state {previous}, got {old_state}",
                max_issues=max_issues,
            )
        if previous in TERMINAL_STATES and new_state and new_state not in TERMINAL_STATES:
            _record_issue(
                issues,
                issue_counts,
                "terminal_state_reopened",
                idx,
                row,
                detail=f"{previous} cannot transition to {new_state}",
                max_issues=max_issues,
            )
        if previous == "LIVE" and new_state == "DISCOVERED":
            _record_issue(
                issues,
                issue_counts,
                "live_to_discovered_regression",
                idx,
                row,
                detail="LIVE must not regress to DISCOVERED during normal reconciliation",
                max_issues=max_issues,
            )
        if previous and new_state and _rank(new_state) < _rank(previous) and not (previous == "STALE" and new_state == "LIVE"):
            _record_issue(
                issues,
                issue_counts,
                "state_rank_regression",
                idx,
                row,
                detail=f"{previous} rank is greater than {new_state}",
                max_issues=max_issues,
            )
        if new_state == "RESOLVED" and not _has_resolution_truth(row):
            _record_issue(
                issues,
                issue_counts,
                "resolved_missing_truth",
                idx,
                row,
                detail="RESOLVED needs winning_asset_id, winning_outcome, or resolution truth metadata",
                max_issues=max_issues,
            )
        if new_state:
            final_states[asset_id] = new_state
        elif previous is None:
            final_states[asset_id] = "UNKNOWN"

        if event_type == "MARKET_RESOLVED" and new_state != "RESOLVED":
            _record_issue(
                issues,
                issue_counts,
                "resolved_event_without_resolved_state",
                idx,
                row,
                detail="MARKET_RESOLVED should end in RESOLVED state",
                max_issues=max_issues,
            )

    status = "PASS" if not issue_counts else "WARN"
    return LifecycleReplayReport(
        status=status,
        events_replayed=len(rows),
        assets_seen=len(final_states),
        final_states=dict(sorted(final_states.items())),
        issue_counts=dict(sorted(issue_counts.items())),
        issues=issues,
    )


def load_lifecycle_jsonl(path: str | Path) -> list[dict[str, Any]]:
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", required=True, type=Path)
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--max-issues", type=int, default=100)
    args = parser.parse_args(argv)
    report = replay_lifecycle_events(load_lifecycle_jsonl(args.jsonl), max_issues=args.max_issues)
    text = json.dumps(report.as_dict(), ensure_ascii=False, indent=2, sort_keys=True, default=str)
    print(text)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(text + "\n", encoding="utf-8")
    return 0 if report.status == "PASS" else 1


def _record_issue(
    issues: list[LifecycleReplayIssue],
    counts: defaultdict[str, int],
    issue_type: str,
    event_index: int,
    row: Mapping[str, Any],
    *,
    detail: str = "",
    max_issues: int,
) -> None:
    counts[issue_type] += 1
    if len(issues) >= max(0, int(max_issues)):
        return
    issues.append(
        LifecycleReplayIssue(
            asset_id=str(row.get("asset_id") or row.get("token_id") or "").strip(),
            issue_type=issue_type,
            event_index=event_index,
            old_state=_state(row.get("old_state")),
            new_state=_state(row.get("new_state") or row.get("market_state")),
            event_type=str(row.get("event_type") or "").strip() or None,
            detail=detail,
        )
    )


def _state(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _rank(state: str) -> int:
    return STATE_RANK.get(_state(state) or "", -1)


def _has_resolution_truth(row: Mapping[str, Any]) -> bool:
    if row.get("winning_asset_id") or row.get("winning_outcome") or row.get("resolution_truth"):
        return True
    meta = row.get("meta")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    if not isinstance(meta, Mapping):
        return False
    return bool(
        meta.get("winning_asset_id")
        or meta.get("winning_outcome")
        or meta.get("resolution_truth")
        or meta.get("oracle_resolution")
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
