"""Fail-closed finalization of a C/D-quorum repair into an immutable Gold overlay.

This module deliberately does not edit Source-A/Silver Parquet.  It consumes
the bounded artifacts emitted by ``run_gcp_l2_three_source_sparse_repair.sh``,
replays the candidate against explicit hash-bound Source-A state evidence, and
publishes a separate immutable overlay only when every contract agrees.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

import duckdb

from .gold_hour_validator import validate_gold_hour
from .l2_replay import (
    L2ReplayNotReady,
    _levels_from_json,
    _validate_uncrossed_book,
    apply_price_change_group_with_top_fence,
)

FORMAL_REQUEST_ORIGIN = "assignment_ledger_primary_closed_gap"
REQUEST_SCHEMA = "polymarket-l2-gap-repair-request-v1"
COMPARISON_SCHEMA = "polymarket-l2-parquet-source-set-comparison-v2"
CANDIDATE_RECEIPT_SCHEMA = "polymarket-l2-three-source-sparse-repair-receipt-v1"
CONTINUITY_SCHEMA = "polymarket-l2-three-source-continuity-proof-v1"
OBSERVER_CONTINUITY_SCHEMA = "polymarket-l2-observer-assignment-continuity-v1"
COMPLETENESS_SCHEMA = "polymarket-l2-source-completeness-proof-v1"
STATE_INPUT_SCHEMA = "polymarket-l2-source-a-state-input-v1"
STATE_INPUT_SCHEMA_V2 = "polymarket-l2-source-a-state-input-v2"
CONSENSUS_PRE_GAP_STATE = "C_D_CONSENSUS_PRE_GAP_STATE"
STATE_REPLAY_SCHEMA = "polymarket-l2-three-source-state-replay-v1"
OVERLAY_SCHEMA = "polymarket-l2-three-source-gold-overlay-v1"
EVENT_REPAIR_MODE = "EVENT_REPAIR"
CONTINUITY_ONLY_MODE = "CONTINUITY_ONLY"


class ThreeSourceGoldCommitError(RuntimeError):
    """Raised when any repair or state-proof invariant is unproven."""


@dataclass(frozen=True, slots=True)
class _RequestWindow:
    request_id: str
    asset_id: str
    hour_start: datetime
    gap_start: datetime
    recovered_at: datetime
    primary_shard_ids: tuple[int, ...]


def finalize_three_source_gold_candidate(
    *,
    request_path: Path,
    candidate_receipt_path: Path,
    comparison_report_path: Path,
    candidate_path: Path | None,
    continuity_proof_path: Path,
    observer_continuity_receipts: Mapping[str, Path],
    source_completeness_proofs: Mapping[str, Path],
    source_a_state_root: Path,
    source_a_state_manifest_path: Path,
    output_dir: Path,
    memory_limit: str = "8GB",
    max_post_checkpoint_delay_seconds: int = 300,
) -> dict[str, Any]:
    """Validate and publish one immutable C/D-quorum overlay.

    All input paths are explicit.  In particular, state replay never searches
    an archive or guesses a shard: ``source_a_state_manifest_path`` must bind
    every Parquet file needed for the pre-gap baseline, intervening A events,
    and post-recovery A checkpoint.
    """

    inputs = {
        "request": Path(request_path),
        "candidate_receipt": Path(candidate_receipt_path),
        "comparison": Path(comparison_report_path),
        "continuity": Path(continuity_proof_path),
        "state_manifest": Path(source_a_state_manifest_path),
    }
    for label, path in inputs.items():
        _require_regular_file(path, label)

    request = _load_json(inputs["request"], "request")
    request_sha = _sha256_file(inputs["request"])
    hour, windows = _validate_request(request)
    request_ids = tuple(window.request_id for window in windows)

    receipt = _load_json(inputs["candidate_receipt"], "candidate receipt")
    comparison = _load_json(inputs["comparison"], "comparison report")
    continuity = _load_json(inputs["continuity"], "continuity proof")
    mode = _receipt_mode(receipt)
    resolved_candidate: Path | None = None
    candidate_sha: str | None = None
    if candidate_path is not None:
        resolved_candidate = Path(candidate_path)
        _require_regular_file(resolved_candidate, "candidate")
        candidate_sha = _sha256_file(resolved_candidate)
    comparison_sha = _sha256_file(inputs["comparison"])
    continuity_sha = _sha256_file(inputs["continuity"])

    observer_hashes = _validate_observer_continuity(
        paths=observer_continuity_receipts,
        request_sha=request_sha,
        request_ids=request_ids,
    )
    completeness_hashes = _validate_candidate_contract(
        request=request,
        request_sha=request_sha,
        windows=windows,
        receipt=receipt,
        comparison=comparison,
        comparison_path=inputs["comparison"],
        comparison_sha=comparison_sha,
        candidate_path=resolved_candidate,
        candidate_sha=candidate_sha,
        continuity=continuity,
        continuity_sha=continuity_sha,
        observer_continuity_sha=observer_hashes,
        completeness_paths=source_completeness_proofs,
        mode=mode,
    )
    if mode == EVENT_REPAIR_MODE:
        assert resolved_candidate is not None and candidate_sha is not None
        candidate_rows = _load_and_validate_candidate_rows(
            path=resolved_candidate,
            windows=windows,
            comparison=comparison,
            memory_limit=memory_limit,
        )
    else:
        candidate_sha = _continuity_only_binding_sha256(
            request_sha=request_sha,
            comparison_sha=comparison_sha,
            continuity_sha=continuity_sha,
            observer_hashes=observer_hashes,
            completeness_hashes=completeness_hashes,
            state_proof_sha=str(comparison.get("state_proof_sha256") or ""),
        )
        candidate_rows = []
    assert candidate_sha is not None
    state_files, state_manifest_sha = _validate_state_manifest(
        root=Path(source_a_state_root),
        manifest_path=inputs["state_manifest"],
        request_sha=request_sha,
        observer_continuity_sha=observer_hashes,
    )
    try:
        state_receipt = _build_state_replay_receipt(
            request_sha=request_sha,
            candidate_sha=candidate_sha,
            state_manifest_sha=state_manifest_sha,
            state_files=state_files,
            state_manifest=_load_json(inputs["state_manifest"], "state manifest"),
            windows=windows,
            candidate_rows=candidate_rows,
            memory_limit=memory_limit,
            max_post_checkpoint_delay_seconds=max_post_checkpoint_delay_seconds,
            mode=mode,
        )
    except L2ReplayNotReady as exc:
        raise ThreeSourceGoldCommitError(
            f"Source-A state replay failed closed: {exc}"
        ) from exc
    _validate_state_replay_receipt(
        state_receipt,
        request_sha=request_sha,
        candidate_sha=candidate_sha,
        state_manifest_sha=state_manifest_sha,
        request_ids=request_ids,
        candidate_row_count=len(candidate_rows),
        mode=mode,
    )

    idempotency_key = _idempotency_key(request_ids, candidate_sha)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    tag = hour.strftime("%Y%m%dT%H")
    stem = f"three_source_gold_overlay_{tag}_{idempotency_key[:20]}"
    overlay_path = output_dir / f"{stem}.parquet"
    state_receipt_path = output_dir / f"{stem}.state-replay.json"
    manifest_path = output_dir / f"{stem}.manifest.json"
    lock_path = output_dir / ".three_source_gold_commit.lock"

    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ThreeSourceGoldCommitError(
                "the same Gold overlay is already being finalized"
            ) from exc
        if manifest_path.exists():
            return _validate_existing_commit(
                manifest_path=manifest_path,
                overlay_path=overlay_path,
                state_receipt_path=state_receipt_path,
                hour=hour,
                idempotency_key=idempotency_key,
                request_sha=request_sha,
                candidate_sha=candidate_sha,
                comparison_sha=comparison_sha,
                continuity_sha=continuity_sha,
                state_manifest_sha=state_manifest_sha,
                memory_limit=memory_limit,
                mode=mode,
            )
        if overlay_path.exists() or state_receipt_path.exists():
            raise ThreeSourceGoldCommitError(
                "orphaned immutable Gold artifact exists without its manifest"
            )

        state_bytes = _json_bytes(state_receipt)
        state_receipt_sha = hashlib.sha256(state_bytes).hexdigest()
        if mode == EVENT_REPAIR_MODE:
            assert resolved_candidate is not None
            overlay_summary = _write_overlay_atomic(
                candidate_path=resolved_candidate,
                windows=windows,
                output_path=overlay_path,
                hour=hour,
                idempotency_key=idempotency_key,
                candidate_sha=candidate_sha,
                state_receipt_sha=state_receipt_sha,
                memory_limit=memory_limit,
            )
        else:
            overlay_summary = _write_continuity_overlay_atomic(
                schema_paths=state_files,
                output_path=overlay_path,
                hour=hour,
                idempotency_key=idempotency_key,
                candidate_sha=candidate_sha,
                state_receipt_sha=state_receipt_sha,
                memory_limit=memory_limit,
            )
        _atomic_write_bytes(state_receipt_path, state_bytes)
        repair_windows = [_portable_repair_window(window) for window in windows]
        portable_overlay = dict(overlay_summary)
        portable_overlay["path"] = overlay_path.name
        manifest = {
            "schema_version": OVERLAY_SCHEMA,
            "status": "PASS",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "hour_start": hour.isoformat(),
            "request_origin": FORMAL_REQUEST_ORIGIN,
            "request_ids": list(request_ids),
            "request_sha256": request_sha,
            "repair_windows": repair_windows,
            "repair_windows_binding_sha256": _repair_windows_binding_sha256(
                request_sha=request_sha,
                repair_windows=repair_windows,
            ),
            "candidate_receipt_sha256": _sha256_file(inputs["candidate_receipt"]),
            "comparison_sha256": comparison_sha,
            "candidate_sha256": candidate_sha,
            "continuity_proof_sha256": continuity_sha,
            "observer_continuity_sha256": observer_hashes,
            "source_completeness_proof_sha256": completeness_hashes,
            "source_a_state_manifest_sha256": state_manifest_sha,
            "state_replay_receipt": {
                "path": state_receipt_path.name,
                "sha256": state_receipt_sha,
            },
            "idempotency_key": idempotency_key,
            "canonical_source": "C",
            "supporting_sources": ["C", "D"],
            "source_mask": 24,
            "repair_status": "EVENT_REPAIRED_QUORUM",
            "evidence_level": 2,
            "overlay": portable_overlay,
            "base_source_a_mutated": False,
            "silver_mutated": False,
        }
        if mode == CONTINUITY_ONLY_MODE:
            manifest.update(
                {
                    "mode": CONTINUITY_ONLY_MODE,
                    "repair_status": CONTINUITY_ONLY_MODE,
                    "canonical_repair_applied": False,
                    "coverage_repair_proven": True,
                    "zero_event_request_count": len(windows),
                    "candidate_materialization": "EMPTY_PARQUET_BY_CONTINUITY_PROOF",
                    "comparison_state_proof_sha256": comparison.get(
                        "state_proof_sha256"
                    ),
                }
            )
        _atomic_write_bytes(manifest_path, _json_bytes(manifest))

        # The same self-contained validator used by coverage/replay must accept
        # a newly published overlay before this call can report success.
        validate_three_source_gold_manifest(
            manifest_path=manifest_path,
            memory_limit=memory_limit,
        )

    result = _manifest_result_with_resolved_artifacts(
        manifest,
        overlay_path=overlay_path,
        state_receipt_path=state_receipt_path,
    )
    result["manifest_path"] = str(manifest_path)
    result["manifest_sha256"] = _sha256_file(manifest_path)
    result["idempotent_replay"] = False
    return result


def validate_three_source_gold_manifest(
    *, manifest_path: Path, memory_limit: str = "8GB"
) -> dict[str, Any]:
    """Strictly validate a committed overlay using only its Gold manifest.

    Coverage and replay callers do not need to retain the coordinator's input
    arguments.  The manifest carries portable repair windows and transitive
    hashes; this validator reopens the overlay and state receipt, verifies
    their SHAs and semantics, and rejects audit/non-quorum provenance.
    """

    try:
        return _validate_three_source_gold_manifest(
            manifest_path=manifest_path,
            memory_limit=memory_limit,
        )
    except ThreeSourceGoldCommitError:
        raise
    except (duckdb.Error, OSError, TypeError, ValueError) as exc:
        raise ThreeSourceGoldCommitError(
            f"Gold manifest validation failed closed: {exc}"
        ) from exc


def _validate_three_source_gold_manifest(
    *, manifest_path: Path, memory_limit: str
) -> dict[str, Any]:
    path = Path(manifest_path)
    _require_regular_file(path, "Gold manifest")
    try:
        manifest_bytes = path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ThreeSourceGoldCommitError(f"invalid Gold manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ThreeSourceGoldCommitError("Gold manifest must be a JSON object")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    request_sha = str(manifest.get("request_sha256") or "")
    candidate_sha = str(manifest.get("candidate_sha256") or "")
    state_manifest_sha = str(manifest.get("source_a_state_manifest_sha256") or "")
    mode = str(manifest.get("mode") or EVENT_REPAIR_MODE)
    if (
        manifest.get("schema_version") != OVERLAY_SCHEMA
        or manifest.get("status") != "PASS"
        or manifest.get("request_origin") != FORMAL_REQUEST_ORIGIN
        or manifest.get("canonical_source") != "C"
        or tuple(manifest.get("supporting_sources") or ()) != ("C", "D")
        or int(manifest.get("source_mask", -1)) != 24
        or int(manifest.get("evidence_level", -1)) != 2
        or manifest.get("base_source_a_mutated") is not False
        or manifest.get("silver_mutated") is not False
        or not all(
            _valid_sha(value)
            for value in (
                request_sha,
                manifest.get("candidate_receipt_sha256"),
                manifest.get("comparison_sha256"),
                candidate_sha,
                manifest.get("continuity_proof_sha256"),
                state_manifest_sha,
            )
        )
    ):
        raise ThreeSourceGoldCommitError(
            "Gold manifest is not a formal C/D-quorum overlay contract"
        )
    if mode == EVENT_REPAIR_MODE:
        if manifest.get("repair_status") != "EVENT_REPAIRED_QUORUM":
            raise ThreeSourceGoldCommitError(
                "Gold manifest is not an event-repair overlay contract"
            )
    elif mode == CONTINUITY_ONLY_MODE:
        if (
            manifest.get("repair_status") != CONTINUITY_ONLY_MODE
            or manifest.get("canonical_repair_applied") is not False
            or manifest.get("coverage_repair_proven") is not True
            or manifest.get("candidate_materialization")
            != "EMPTY_PARQUET_BY_CONTINUITY_PROOF"
            or not _valid_sha(manifest.get("comparison_state_proof_sha256"))
        ):
            raise ThreeSourceGoldCommitError(
                "Gold continuity-only manifest has an invalid proof contract"
            )
    else:
        raise ThreeSourceGoldCommitError("Gold manifest mode is unsupported")
    for key, expected in (
        ("observer_continuity_sha256", {"C", "D"}),
        ("source_completeness_proof_sha256", {"A", "C", "D"}),
    ):
        hashes = manifest.get(key)
        if (
            not isinstance(hashes, dict)
            or set(hashes) != expected
            or not all(_valid_sha(value) for value in hashes.values())
        ):
            raise ThreeSourceGoldCommitError(f"Gold manifest has invalid {key}")
    if mode == CONTINUITY_ONLY_MODE:
        expected_candidate_sha = _continuity_only_binding_sha256(
            request_sha=request_sha,
            comparison_sha=str(manifest["comparison_sha256"]),
            continuity_sha=str(manifest["continuity_proof_sha256"]),
            observer_hashes=manifest["observer_continuity_sha256"],
            completeness_hashes=manifest["source_completeness_proof_sha256"],
            state_proof_sha=str(manifest["comparison_state_proof_sha256"]),
        )
        if candidate_sha != expected_candidate_sha:
            raise ThreeSourceGoldCommitError(
                "Gold continuity-only proof binding SHA is invalid"
            )

    hour, windows = _validate_portable_repair_windows(manifest)
    request_ids = tuple(window.request_id for window in windows)
    portable = [_portable_repair_window(window) for window in windows]
    expected_binding = _repair_windows_binding_sha256(
        request_sha=request_sha,
        repair_windows=portable,
    )
    if manifest.get("repair_windows_binding_sha256") != expected_binding:
        raise ThreeSourceGoldCommitError(
            "Gold repair windows are not bound to the request SHA"
        )
    idempotency_key = _idempotency_key(request_ids, candidate_sha)
    if manifest.get("idempotency_key") != idempotency_key:
        raise ThreeSourceGoldCommitError("Gold idempotency key is invalid")

    overlay = manifest.get("overlay")
    state_pointer = manifest.get("state_replay_receipt")
    if not isinstance(overlay, dict) or not isinstance(state_pointer, dict):
        raise ThreeSourceGoldCommitError("Gold artifact pointers are missing")
    overlay_path = _manifest_artifact_path(overlay.get("path"), path, "overlay")
    state_path = _manifest_artifact_path(
        state_pointer.get("path"), path, "state replay receipt"
    )
    overlay_sha = _sha256_file(overlay_path)
    if overlay_sha != overlay.get("sha256") or overlay_path.stat().st_size != int(
        overlay.get("bytes", -1)
    ):
        raise ThreeSourceGoldCommitError("Gold artifact SHA/size mismatch")

    state = _load_sha_bound_json(
        state_path,
        expected_sha=str(state_pointer.get("sha256") or ""),
        label="state replay receipt",
    )
    overlay_row_count = int(overlay.get("row_count", -1))
    _validate_state_replay_receipt(
        state,
        request_sha=request_sha,
        candidate_sha=candidate_sha,
        state_manifest_sha=state_manifest_sha,
        request_ids=request_ids,
        candidate_row_count=overlay_row_count,
        mode=mode,
    )
    _validate_state_receipt_windows(state=state, windows=windows, mode=mode)
    overlay_summary = _validate_committed_overlay(
        path=overlay_path,
        hour=hour,
        windows=windows,
        candidate_sha=candidate_sha,
        idempotency_key=idempotency_key,
        state_receipt_sha=str(state_pointer["sha256"]),
        declared=overlay,
        memory_limit=memory_limit,
        mode=mode,
    )
    if overlay_summary["sha256"] != overlay_sha:
        raise ThreeSourceGoldCommitError("Gold overlay changed during validation")
    if _sha256_file(path) != manifest_sha:
        raise ThreeSourceGoldCommitError("Gold manifest changed during validation")
    result = {
        "schema_version": "polymarket-l2-three-source-gold-validation-v1",
        "status": "PASS",
        "manifest_path": str(path),
        "manifest_sha256": manifest_sha,
        "hour_start": hour.isoformat(),
        "request_sha256": request_sha,
        "request_count": len(windows),
        "zero_event_request_count": sum(
            int(row.get("candidate_price_change_rows", -1)) == 0
            for row in state["requests"]
        ),
        "repair_windows": portable,
        "resolved_overlay_path": str(overlay_path),
        "resolved_state_replay_receipt_path": str(state_path),
        "overlay": overlay_summary,
    }
    if mode == CONTINUITY_ONLY_MODE:
        if int(manifest.get("zero_event_request_count", -1)) != len(windows):
            raise ThreeSourceGoldCommitError(
                "Gold continuity-only request count is invalid"
            )
        result.update(
            {
                "mode": CONTINUITY_ONLY_MODE,
                "canonical_repair_applied": False,
                "coverage_repair_proven": True,
            }
        )
    return result


def _validate_request(
    payload: Mapping[str, Any],
) -> tuple[datetime, tuple[_RequestWindow, ...]]:
    if payload.get("schema_version") != REQUEST_SCHEMA:
        raise ThreeSourceGoldCommitError("unsupported repair request schema")
    if payload.get("request_origin") != FORMAL_REQUEST_ORIGIN:
        raise ThreeSourceGoldCommitError(
            "Gold finalization only accepts formal closed-gap requests"
        )
    if payload.get("request_mode") == "AUDIT_ONLY":
        raise ThreeSourceGoldCommitError("audit requests can never mutate Gold")
    rows = payload.get("requests")
    if not isinstance(rows, list) or not rows:
        raise ThreeSourceGoldCommitError("repair request list is empty")
    if int(payload.get("request_count", -1)) != len(rows):
        raise ThreeSourceGoldCommitError("repair request_count mismatch")
    windows: list[_RequestWindow] = []
    ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ThreeSourceGoldCommitError("repair request row is not an object")
        request_id = str(row.get("request_id") or "").strip()
        asset_id = str(row.get("asset_id") or "").strip()
        if not request_id or request_id in ids or not asset_id:
            raise ThreeSourceGoldCommitError(
                "repair request IDs/assets must be nonempty and unique"
            )
        ids.add(request_id)
        hour = _floor_hour(_parse_datetime(row.get("hour_start")))
        gap_start = _parse_datetime(row.get("gap_start"))
        recovered_at = _parse_datetime(row.get("recovered_at"))
        raw_shards = row.get("primary_shard_ids")
        if (
            not isinstance(raw_shards, list)
            or not raw_shards
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                or value >= 48
                for value in raw_shards
            )
        ):
            raise ThreeSourceGoldCommitError(
                f"request {request_id} lacks valid primary shard provenance"
            )
        primary_shard_ids = tuple(sorted(set(raw_shards)))
        if not (hour <= gap_start < recovered_at <= hour + timedelta(hours=1)):
            raise ThreeSourceGoldCommitError(
                f"request {request_id} is not a positive single-hour gap"
            )
        windows.append(
            _RequestWindow(
                request_id=request_id,
                asset_id=asset_id,
                hour_start=hour,
                gap_start=gap_start,
                recovered_at=recovered_at,
                primary_shard_ids=primary_shard_ids,
            )
        )
    hours = {row.hour_start for row in windows}
    if len(hours) != 1:
        raise ThreeSourceGoldCommitError("Gold commit requires one request hour")
    by_asset: dict[str, list[_RequestWindow]] = defaultdict(list)
    for window in windows:
        by_asset[window.asset_id].append(window)
    for asset_windows in by_asset.values():
        ordered = sorted(asset_windows, key=lambda row: row.gap_start)
        for left, right in pairwise(ordered):
            if right.gap_start < left.recovered_at:
                raise ThreeSourceGoldCommitError(
                    "overlapping request windows are ambiguous"
                )
    return next(iter(hours)), tuple(sorted(windows, key=lambda row: row.request_id))


def _portable_repair_window(window: _RequestWindow) -> dict[str, Any]:
    return {
        "request_id": window.request_id,
        "asset_id": window.asset_id,
        "hour_start": window.hour_start.isoformat(),
        "gap_start": window.gap_start.isoformat(),
        "recovered_at": window.recovered_at.isoformat(),
        "primary_shard_ids": list(window.primary_shard_ids),
    }


def _repair_windows_binding_sha256(
    *, request_sha: str, repair_windows: Sequence[Mapping[str, Any]]
) -> str:
    return hashlib.sha256(
        _json_bytes(
            {
                "request_sha256": request_sha,
                "repair_windows": list(repair_windows),
            }
        )
    ).hexdigest()


def _continuity_only_binding_sha256(
    *,
    request_sha: str,
    comparison_sha: str,
    continuity_sha: str,
    observer_hashes: Mapping[str, str],
    completeness_hashes: Mapping[str, str],
    state_proof_sha: str,
) -> str:
    """Return a stable candidate identity for a proven empty event set.

    A continuity-only commit has no candidate file.  Its compatibility
    ``candidate_sha256`` is therefore the digest of every transitive proof
    that establishes the empty set, rather than the SHA of an invented row.
    """

    payload = {
        "schema_version": "polymarket-l2-continuity-only-binding-v1",
        "request_sha256": request_sha,
        "comparison_sha256": comparison_sha,
        "continuity_proof_sha256": continuity_sha,
        "observer_continuity_sha256": dict(sorted(observer_hashes.items())),
        "source_completeness_proof_sha256": dict(
            sorted(completeness_hashes.items())
        ),
        "comparison_state_proof_sha256": state_proof_sha,
    }
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _validate_portable_repair_windows(
    manifest: Mapping[str, Any],
) -> tuple[datetime, tuple[_RequestWindow, ...]]:
    rows = manifest.get("repair_windows")
    if not isinstance(rows, list) or not rows:
        raise ThreeSourceGoldCommitError("Gold manifest has no portable repair windows")
    windows: list[_RequestWindow] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ThreeSourceGoldCommitError("Gold repair window is malformed")
        request_id = str(row.get("request_id") or "").strip()
        asset_id = str(row.get("asset_id") or "").strip()
        hour = _parse_datetime(row.get("hour_start"))
        gap_start = _parse_datetime(row.get("gap_start"))
        recovered_at = _parse_datetime(row.get("recovered_at"))
        raw_shards = row.get("primary_shard_ids")
        valid_shards = (
            isinstance(raw_shards, list)
            and bool(raw_shards)
            and all(
                not isinstance(value, bool)
                and isinstance(value, int)
                and 0 <= value < 48
                for value in raw_shards
            )
        )
        normalized_shards = (
            tuple(sorted(set(raw_shards)))
            if valid_shards and isinstance(raw_shards, list)
            else ()
        )
        if (
            not request_id
            or request_id in seen
            or not asset_id
            or hour != _floor_hour(hour)
            or not (hour <= gap_start < recovered_at <= hour + timedelta(hours=1))
            or not valid_shards
            or list(normalized_shards) != raw_shards
        ):
            raise ThreeSourceGoldCommitError("Gold repair window is invalid")
        seen.add(request_id)
        windows.append(
            _RequestWindow(
                request_id=request_id,
                asset_id=asset_id,
                hour_start=hour,
                gap_start=gap_start,
                recovered_at=recovered_at,
                primary_shard_ids=tuple(int(value) for value in normalized_shards),
            )
        )
    ordered = tuple(sorted(windows, key=lambda row: row.request_id))
    hours = {row.hour_start for row in ordered}
    if (
        len(hours) != 1
        or _parse_datetime(manifest.get("hour_start")) != next(iter(hours))
        or sorted(manifest.get("request_ids") or [])
        != [row.request_id for row in ordered]
    ):
        raise ThreeSourceGoldCommitError(
            "Gold repair windows disagree with manifest scope"
        )
    by_asset: dict[str, list[_RequestWindow]] = defaultdict(list)
    for window in ordered:
        by_asset[window.asset_id].append(window)
    for asset_windows in by_asset.values():
        for left, right in pairwise(
            sorted(asset_windows, key=lambda row: row.gap_start)
        ):
            if right.gap_start < left.recovered_at:
                raise ThreeSourceGoldCommitError(
                    "Gold repair windows overlap for one asset"
                )
    return next(iter(hours)), ordered


def _validate_observer_continuity(
    *,
    paths: Mapping[str, Path],
    request_sha: str,
    request_ids: Sequence[str],
) -> dict[str, str]:
    if set(paths) != {"C", "D"}:
        raise ThreeSourceGoldCommitError(
            "observer continuity receipts must contain exactly C and D"
        )
    result: dict[str, str] = {}
    expected_ids = sorted(request_ids)
    for source in ("C", "D"):
        path = Path(paths[source])
        _require_regular_file(path, f"{source} continuity receipt")
        payload = _load_json(path, f"{source} continuity receipt")
        if (
            payload.get("schema_version") != OBSERVER_CONTINUITY_SCHEMA
            or payload.get("status") != "PASS"
            or payload.get("standby_source_id") != source
            or payload.get("parent_request_sha256") != request_sha
            or payload.get("observer_assignment_policy") != "EXACT_CONTINUITY_ONLY"
            or int(payload.get("accepted_request_count", -1)) != len(expected_ids)
            or int(payload.get("rejected_request_count", -1)) != 0
            or int(payload.get("exact_continuity_request_count", -1))
            != len(expected_ids)
            or int(payload.get("operational_bounded_gap_request_count", -1)) != 0
            or not _is_one(payload.get("accepted_assignment_time_coverage"))
            or not _is_one(payload.get("exact_continuity_time_coverage"))
            or sorted(payload.get("accepted_request_ids") or []) != expected_ids
        ):
            raise ThreeSourceGoldCommitError(
                f"{source} observer continuity is not exact and request-bound"
            )
        result[source] = _sha256_file(path)
    return result


def _receipt_mode(receipt: Mapping[str, Any]) -> str:
    status = receipt.get("status")
    if status == "CANDIDATE_READY_REQUIRES_A_STATE_REPLAY":
        return EVENT_REPAIR_MODE
    if status == "VERIFIED_NO_REPAIR_NEEDED":
        return CONTINUITY_ONLY_MODE
    raise ThreeSourceGoldCommitError(
        "coordinator receipt is not a supported formal Gold input"
    )


def _validate_candidate_contract(
    *,
    request: Mapping[str, Any],
    request_sha: str,
    windows: Sequence[_RequestWindow],
    receipt: Mapping[str, Any],
    comparison: Mapping[str, Any],
    comparison_path: Path,
    comparison_sha: str,
    candidate_path: Path | None,
    candidate_sha: str | None,
    continuity: Mapping[str, Any],
    continuity_sha: str,
    observer_continuity_sha: Mapping[str, str],
    completeness_paths: Mapping[str, Path],
    mode: str,
) -> dict[str, str]:
    count = len(windows)
    request_ids = sorted(row.request_id for row in windows)
    batch_id = request.get("batch_id")
    common_receipt_invalid = (
        receipt.get("schema_version") != CANDIDATE_RECEIPT_SCHEMA
        or receipt.get("request_origin") != FORMAL_REQUEST_ORIGIN
        or receipt.get("batch_id") != batch_id
        or receipt.get("request_sha256") != request_sha
        or receipt.get("comparison_sha256") != comparison_sha
        or receipt.get("continuity_proof_sha256") != continuity_sha
        or receipt.get("canonical_repair_applied") is not False
        or receipt.get("canonical_repair_eligible") is not False
        or receipt.get("unresolved_events_remain_fail_closed") is not False
        or receipt.get("observer_assignment_policy") != "EXACT_C_D_CONTINUITY"
        or not _is_one(receipt.get("accepted_request_coverage"))
        or not _is_one(receipt.get("c_assignment_time_coverage"))
        or not _is_one(receipt.get("d_assignment_time_coverage"))
        or not _is_one(receipt.get("accepted_assignment_time_coverage"))
        or not _is_one(receipt.get("exact_continuity_time_coverage"))
        or int(receipt.get("exact_continuity_request_count", -1)) != count
        or int(receipt.get("operational_bounded_gap_request_count", -1)) != 0
    )
    if common_receipt_invalid:
        raise ThreeSourceGoldCommitError(
            "candidate receipt is not an exact formal C/D repair contract"
        )
    _require_same_path(receipt.get("comparison_report"), comparison_path, "comparison")
    single_source_unresolved = int(
        comparison.get("single_source_unresolved_events", -1)
    )
    source_a_only = int(comparison.get("source_a_only_events", -1))
    quorum_events = int(comparison.get("quorum_events", -1))
    union_events = int(comparison.get("union_events", -1))
    common_comparison_invalid = (
        comparison.get("schema_version") != COMPARISON_SCHEMA
        or comparison.get("comparison_scope") != "REQUEST_ASSET_GAP_WINDOWS"
        or tuple(comparison.get("sources") or ()) != ("A", "C", "D")
        or comparison.get("request_sha256") != request_sha
        or int(comparison.get("request_count", -1)) != count
        or comparison.get("continuity_proof_sha256") != continuity_sha
        or single_source_unresolved < 0
        or source_a_only < 0
        or quorum_events < 0
        or union_events < 0
        or single_source_unresolved != source_a_only
        or int(comparison.get("ambiguous_match_events", -1)) != 0
        or int(comparison.get("temporal_payload_similarity_events", -1)) != 0
        or quorum_events + source_a_only != union_events
        or comparison.get("input_errors") not in ([], ())
    )
    if common_comparison_invalid:
        raise ThreeSourceGoldCommitError(
            "comparison does not prove a resolved strong C/D quorum"
        )
    if mode == EVENT_REPAIR_MODE:
        if (
            receipt.get("status") != "CANDIDATE_READY_REQUIRES_A_STATE_REPLAY"
            or receipt.get("comparison_status") != "candidate-ready"
            or receipt.get("candidate_sha256") != candidate_sha
            or candidate_path is None
            or not _valid_sha(candidate_sha)
            or comparison.get("status") != "candidate-ready"
            or comparison.get("repair_candidate_sha256") != candidate_sha
            or int(comparison.get("repair_candidate_events", 0)) <= 0
        ):
            raise ThreeSourceGoldCommitError(
                "candidate receipt is not an event-repair Gold contract"
            )
        _require_same_path(receipt.get("candidate_path"), candidate_path, "candidate")
        _require_same_path(
            comparison.get("repair_candidate_path"),
            candidate_path,
            "comparison candidate",
        )
    elif mode == CONTINUITY_ONLY_MODE:
        state_proof_path = str(receipt.get("state_proof") or "").strip()
        state_proof_sha = str(comparison.get("state_proof_sha256") or "")
        if (
            receipt.get("status") != "VERIFIED_NO_REPAIR_NEEDED"
            or receipt.get("comparison_status") != "fully-matched"
            or receipt.get("candidate_path") is not None
            or receipt.get("candidate_sha256") is not None
            or candidate_path is not None
            or comparison.get("status") != "fully-matched"
            or comparison.get("status_reason")
            != "quiet_window_external_continuity_and_state_proven"
            or comparison.get("repair_candidate_path") is not None
            or comparison.get("repair_candidate_sha256") is not None
            or int(comparison.get("repair_candidate_events", -1)) != 0
            or int(comparison.get("repair_candidate_tokens", -1)) != 0
            or int(comparison.get("union_events", -1)) != 0
            or int(comparison.get("quorum_events", -1)) != 0
            or comparison.get("source_events") != {"A": 0, "C": 0, "D": 0}
            or not state_proof_path
            or not _valid_sha(state_proof_sha)
        ):
            raise ThreeSourceGoldCommitError(
                "fully-matched receipt is not a strict zero-event continuity proof"
            )
        state_proof = Path(state_proof_path)
        _require_regular_file(state_proof, "comparison state proof")
        if _sha256_file(state_proof) != state_proof_sha:
            raise ThreeSourceGoldCommitError(
                "comparison state proof SHA binding mismatch"
            )
    else:  # pragma: no cover - guarded by _receipt_mode
        raise ThreeSourceGoldCommitError("unsupported Gold finalization mode")

    if (
        continuity.get("schema_version") != CONTINUITY_SCHEMA
        or continuity.get("status") != "PASS"
        or continuity.get("batch_id") != batch_id
        or continuity.get("request_sha256") != request_sha
        or continuity.get("observer_assignment_policy") != "EXACT_C_D_CONTINUITY"
        or sorted(continuity.get("request_ids") or []) != request_ids
        or not _is_one(continuity.get("accepted_request_coverage"))
        or not _is_one(continuity.get("c_assignment_time_coverage"))
        or not _is_one(continuity.get("d_assignment_time_coverage"))
        or not _is_one(continuity.get("accepted_assignment_time_coverage"))
        or not _is_one(continuity.get("exact_continuity_time_coverage"))
        or int(continuity.get("exact_continuity_request_count", -1)) != count
        or int(continuity.get("operational_bounded_gap_request_count", -1)) != 0
        or continuity.get("observer_continuity_sha256") != dict(observer_continuity_sha)
    ):
        raise ThreeSourceGoldCommitError(
            "combined continuity proof is not exact or child-SHA-bound"
        )

    if set(completeness_paths) != {"A", "C", "D"}:
        raise ThreeSourceGoldCommitError(
            "source completeness proofs must contain exactly A/C/D"
        )
    declared_hashes = comparison.get("source_completeness_proof_sha256")
    source_input_hashes = comparison.get("source_input_sha256")
    source_events = comparison.get("source_events")
    if not all(
        isinstance(item, dict)
        for item in (declared_hashes, source_input_hashes, source_events)
    ):
        raise ThreeSourceGoldCommitError("comparison source bindings are missing")
    assert isinstance(declared_hashes, dict)
    assert isinstance(source_input_hashes, dict)
    assert isinstance(source_events, dict)
    result: dict[str, str] = {}
    for source in ("A", "C", "D"):
        path = Path(completeness_paths[source])
        _require_regular_file(path, f"{source} completeness proof")
        digest = _sha256_file(path)
        proof = _load_json(path, f"{source} completeness proof")
        if (
            digest != declared_hashes.get(source)
            or proof.get("schema_version") != COMPLETENESS_SCHEMA
            or proof.get("status") != "COMPLETE"
            or proof.get("source") != source
            or proof.get("request_sha256") != request_sha
            or proof.get("input_sha256") != source_input_hashes.get(source)
            or int(proof.get("price_change_events", -1))
            != int(source_events.get(source, -2))
            or proof.get("wal_window_complete") is not True
            or not _valid_sha(proof.get("raw_manifest_sha256"))
        ):
            raise ThreeSourceGoldCommitError(
                f"{source} completeness proof is not comparison-bound"
            )
        result[source] = digest
    return result


def _load_and_validate_candidate_rows(
    *,
    path: Path,
    windows: Sequence[_RequestWindow],
    comparison: Mapping[str, Any],
    memory_limit: str,
) -> list[dict[str, Any]]:
    rows = _read_parquet_rows([path], memory_limit=memory_limit)
    required = {
        "event_type",
        "asset_id",
        "timestamp",
        "timestamp_received",
        "source",
        "side",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "raw_connection_id",
        "raw_connection_generation",
        "raw_frame_seq",
        "message_index",
        "change_index",
        "group_id",
        "is_last_in_group",
        "raw_frame_complete",
        "occurrence_rank",
        "canonical_candidate_source",
        "repair_reason",
        "repair_status",
    }
    if not rows or not required.issubset(rows[0]):
        raise ThreeSourceGoldCommitError(
            "candidate is empty or lacks canonical/raw provenance columns"
        )
    expected_rows = int(comparison.get("repair_candidate_events", -1))
    if len(rows) != expected_rows:
        raise ThreeSourceGoldCommitError("candidate row count mismatches comparison")
    expected_assets = int(comparison.get("repair_candidate_tokens", -1))
    if len({str(row.get("asset_id")) for row in rows}) != expected_assets:
        raise ThreeSourceGoldCommitError("candidate asset count mismatches comparison")

    for row in rows:
        if (
            str(row.get("event_type") or "").lower() != "price_change"
            or row.get("canonical_candidate_source") != "C"
            or row.get("repair_reason") != "C_D_QUORUM_MISSING_FROM_A"
            or row.get("repair_status") != "CONFIRMED_QUORUM_CANDIDATE"
            or not str(row.get("source") or "").strip()
            or int(row.get("occurrence_rank") or 0) <= 0
            or not str(row.get("group_id") or "").strip()
            or not str(row.get("raw_connection_id") or "").strip()
            or row.get("raw_connection_generation") is None
            or row.get("raw_frame_seq") is None
            or row.get("message_index") is None
            or row.get("change_index") is None
            or row.get("best_bid") is None
            or row.get("best_ask") is None
        ):
            raise ThreeSourceGoldCommitError(
                "candidate contains a non-quorum or incomplete provenance row"
            )
        exchange = _parse_datetime(row.get("timestamp"))
        received = _parse_datetime(row.get("timestamp_received"))
        if exchange > received:
            raise ThreeSourceGoldCommitError("candidate clock is causally inverted")
        request_id = _request_for_row(row, windows)
        if request_id is None:
            raise ThreeSourceGoldCommitError(
                "candidate row is outside or ambiguously inside request windows"
            )
        row["_request_id"] = request_id
    _reject_duplicate_raw_rows(rows, "candidate")
    _validate_atomic_groups(rows, "candidate")
    return rows


def _validate_state_manifest(
    *,
    root: Path,
    manifest_path: Path,
    request_sha: str,
    observer_continuity_sha: Mapping[str, str],
) -> tuple[tuple[Path, ...], str]:
    manifest = _load_json(manifest_path, "state manifest")
    schema = manifest.get("schema_version")
    is_v2 = schema == STATE_INPUT_SCHEMA_V2
    if (
        schema not in {STATE_INPUT_SCHEMA, STATE_INPUT_SCHEMA_V2}
        or manifest.get("status") != "COMPLETE"
        or manifest.get("source_id")
        not in {"A", "A_WITH_C_D_CONSENSUS_FALLBACK"}
        or manifest.get("request_sha256") != request_sha
        or manifest.get("state_scope")
        != (
            "REQUEST_PRE_STATE_TO_A_POST_CHECKPOINTS"
            if is_v2
            else "REQUEST_PRE_TO_POST_CHECKPOINTS"
        )
        or manifest.get("window_complete") is not True
        or not isinstance(manifest.get("allowed_sources"), list)
        or not manifest.get("allowed_sources")
    ):
        raise ThreeSourceGoldCommitError(
            "Source-A state manifest is incomplete or request-unbound"
        )
    if is_v2:
        evidence = manifest.get("observer_state_evidence")
        request_rows = manifest.get("requests")
        if (
            manifest.get("pre_state_contract") != CONSENSUS_PRE_GAP_STATE
            or set(manifest.get("allowed_sources") or []) == {"A"}
            or not isinstance(evidence, dict)
            or set(evidence) != {"C", "D"}
            or not isinstance(request_rows, list)
            or not request_rows
        ):
            raise ThreeSourceGoldCommitError(
                "C/D consensus state manifest lacks its exact evidence contract"
            )
        for source_id in ("C", "D"):
            source_evidence = evidence.get(source_id)
            if (
                not isinstance(source_evidence, dict)
                or source_evidence.get("source_id") != source_id
                or source_evidence.get("continuity_receipt_sha256")
                != observer_continuity_sha.get(source_id)
                or not _valid_sha(source_evidence.get("derived_request_sha256"))
                or not _valid_sha(source_evidence.get("evidence_receipt_sha256"))
                or not _valid_sha(source_evidence.get("raw_manifest_sha256"))
                or not _valid_sha(source_evidence.get("transfer_manifest_sha256"))
                or not _is_one(source_evidence.get("anchor_to_gap_continuity"))
            ):
                raise ThreeSourceGoldCommitError(
                    f"C/D consensus state manifest has invalid {source_id} binding"
                )
        consensus_rows = 0
        for row in request_rows:
            if not isinstance(row, dict):
                raise ThreeSourceGoldCommitError(
                    "C/D consensus state manifest has an unproven request baseline"
                )
            if row.get("pre_state_mode") == CONSENSUS_PRE_GAP_STATE:
                consensus = row.get("consensus_pre_gap_state")
                if not isinstance(consensus, dict):
                    raise ThreeSourceGoldCommitError(
                        "C/D consensus state manifest has an unproven request baseline"
                    )
                _validate_consensus_manifest_state(
                    consensus, str(row.get("asset_id") or "")
                )
                consensus_rows += 1
            elif row.get("pre_state_mode") != "SOURCE_A_PRE_GAP_BOOK" or not isinstance(
                row.get("pre_checkpoint"), dict
            ):
                raise ThreeSourceGoldCommitError(
                    "C/D consensus state manifest has an unproven request baseline"
                )
        if consensus_rows == 0:
            raise ThreeSourceGoldCommitError(
                "v2 state manifest contains no C/D consensus fallback"
            )
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ThreeSourceGoldCommitError("Source-A state manifest has no files")
    try:
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise ThreeSourceGoldCommitError(
            f"Source-A state root is unavailable: {exc}"
        ) from exc
    paths: list[Path] = []
    seen: set[Path] = set()
    for entry in entries:
        if not isinstance(entry, dict) or not _valid_sha(entry.get("sha256")):
            raise ThreeSourceGoldCommitError("invalid Source-A state file entry")
        raw_path = str(entry.get("path") or "").strip()
        if not raw_path:
            raise ThreeSourceGoldCommitError("Source-A state file path is empty")
        candidate = Path(raw_path)
        if is_v2:
            try:
                entry_root = Path(str(entry.get("evidence_root") or "")).resolve(
                    strict=True
                )
            except OSError as exc:
                raise ThreeSourceGoldCommitError(
                    "consensus state evidence root is unavailable"
                ) from exc
            if not entry_root.is_dir() or entry.get("source_id") not in {"A", "C", "D"}:
                raise ThreeSourceGoldCommitError(
                    "consensus state file has invalid source/root provenance"
                )
            candidate = candidate if candidate.is_absolute() else entry_root / candidate
        else:
            entry_root = resolved_root
            candidate = candidate if candidate.is_absolute() else resolved_root / candidate
        try:
            path = candidate.resolve(strict=True)
            path.relative_to(entry_root)
        except (OSError, ValueError) as exc:
            raise ThreeSourceGoldCommitError(
                "Source-A state file must resolve under the declared root"
            ) from exc
        if path in seen or not path.is_file() or path.suffix.lower() != ".parquet":
            raise ThreeSourceGoldCommitError(
                "Source-A state files must be unique readable Parquet files"
            )
        seen.add(path)
        if _sha256_file(path) != entry["sha256"]:
            raise ThreeSourceGoldCommitError("Source-A state file SHA mismatch")
        paths.append(path)
    return tuple(sorted(paths)), _sha256_file(manifest_path)


def _build_state_replay_receipt(
    *,
    request_sha: str,
    candidate_sha: str,
    state_manifest_sha: str,
    state_files: Sequence[Path],
    state_manifest: Mapping[str, Any],
    windows: Sequence[_RequestWindow],
    candidate_rows: Sequence[Mapping[str, Any]],
    memory_limit: str,
    max_post_checkpoint_delay_seconds: int,
    mode: str,
) -> dict[str, Any]:
    max_delay = int(max_post_checkpoint_delay_seconds)
    if max_delay < 0:
        raise ThreeSourceGoldCommitError("max post-checkpoint delay cannot be negative")
    state_rows = _read_parquet_rows(state_files, memory_limit=memory_limit)
    required = {
        "event_type",
        "asset_id",
        "timestamp",
        "timestamp_received",
        "source",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
        "raw_connection_id",
        "raw_connection_generation",
        "raw_frame_seq",
        "message_index",
        "change_index",
        "group_id",
        "is_last_in_group",
        "raw_frame_complete",
    }
    if not state_rows or not required.issubset(state_rows[0]):
        raise ThreeSourceGoldCommitError(
            "Source-A state evidence is empty or lacks replay provenance"
        )
    allowed_sources = {
        str(value) for value in state_manifest.get("allowed_sources") or []
    }
    target_assets = {window.asset_id for window in windows}
    relevant_rows = [
        row for row in state_rows if str(row.get("asset_id") or "") in target_assets
    ]
    if not relevant_rows:
        raise ThreeSourceGoldCommitError("Source-A state evidence has no target rows")
    for row in relevant_rows:
        if str(row.get("source") or "") not in allowed_sources:
            raise ThreeSourceGoldCommitError(
                "Source-A state evidence contains an undeclared source"
            )
        if _parse_datetime(row.get("timestamp")) > _parse_datetime(
            row.get("timestamp_received")
        ):
            raise ThreeSourceGoldCommitError("Source-A state clock is inverted")
    _reject_duplicate_raw_rows(relevant_rows, "Source-A state evidence")
    manifest_requests = {
        str(row.get("request_id")): row
        for row in (state_manifest.get("requests") or [])
        if isinstance(row, dict)
    }
    state_v2 = state_manifest.get("schema_version") == STATE_INPUT_SCHEMA_V2
    source_a_name = str(
        state_manifest.get("source")
        or next(iter(state_manifest.get("allowed_sources") or []), "")
    ).strip()

    receipts: list[dict[str, Any]] = []
    used_candidate_rows = 0
    for window in windows:
        rows = [
            row
            for row in relevant_rows
            if str(row.get("asset_id") or "") == window.asset_id
        ]
        a_rows = [row for row in rows if str(row.get("source") or "") == source_a_name]
        books = [
            row
            for row in a_rows
            if str(row.get("event_type") or "").lower() == "book"
        ]
        pre = [
            row
            for row in books
            if _parse_datetime(row.get("timestamp_received")) <= window.gap_start
        ]
        post = [
            row
            for row in books
            if _parse_datetime(row.get("timestamp_received")) >= window.recovered_at
        ]
        manifest_request = manifest_requests.get(window.request_id)
        use_consensus = bool(
            state_v2
            and isinstance(manifest_request, dict)
            and manifest_request.get("pre_state_mode") == CONSENSUS_PRE_GAP_STATE
        )
        if (not pre and not use_consensus) or not post:
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} lacks an A pre/post book checkpoint"
            )
        if state_v2 and not isinstance(manifest_request, dict):
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} lacks manifest-selected provenance"
            )
        checkpoint = (
            _select_manifest_checkpoint(
                post,
                manifest_request.get("post_checkpoint"),
                window.request_id,
                "post",
            )
            if isinstance(manifest_request, dict)
            else min(post, key=_row_order_key)
        )
        checkpoint_received = _parse_datetime(checkpoint["timestamp_received"])
        delay = (checkpoint_received - window.recovered_at).total_seconds()
        if delay > max_delay:
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} post checkpoint is too late"
            )
        if use_consensus:
            consensus = manifest_request.get("consensus_pre_gap_state")
            if not isinstance(consensus, dict):
                raise ThreeSourceGoldCommitError(
                    f"request {window.request_id} lacks consensus pre-gap state"
                )
            _validate_consensus_manifest_state(consensus, window.asset_id)
            if _parse_datetime(consensus.get("state_at")) != window.gap_start:
                raise ThreeSourceGoldCommitError(
                    f"request {window.request_id} consensus state is not at gap start"
                )
            baseline_received = window.gap_start
            bids = _levels_from_json(json.dumps(consensus.get("bids")))
            asks = _levels_from_json(json.dumps(consensus.get("asks")))
            baseline = None
        else:
            baseline = (
                _select_manifest_checkpoint(
                    pre,
                    manifest_request.get("pre_checkpoint"),
                    window.request_id,
                    "pre",
                )
                if isinstance(manifest_request, dict)
                else max(pre, key=_row_order_key)
            )
            baseline_received = _parse_datetime(baseline["timestamp_received"])
            bids = _levels_from_json(baseline.get("bids"))
            asks = _levels_from_json(baseline.get("asks"))
        intermediate_books = [
            row
            for row in books
            if baseline_received < _parse_datetime(row["timestamp_received"])
            < checkpoint_received
        ]
        if intermediate_books:
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} has an ambiguous intermediate A book"
            )
        if baseline is not None:
            _validate_checkpoint_row(baseline, window.request_id, "pre")
        _validate_checkpoint_row(checkpoint, window.request_id, "post")

        a_changes = [
            row
            for row in a_rows
            if str(row.get("event_type") or "").lower() == "price_change"
            and baseline_received
            < _parse_datetime(row["timestamp_received"])
            < checkpoint_received
        ]
        a_gap_changes = [
            row
            for row in a_rows
            if str(row.get("event_type") or "").lower() == "price_change"
            and window.gap_start
            <= _parse_datetime(row["timestamp_received"])
            < window.recovered_at
        ]
        if mode == CONTINUITY_ONLY_MODE and a_gap_changes:
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} has Source-A events in a zero-event gap"
            )
        request_candidates = [
            dict(row)
            for row in candidate_rows
            if row.get("_request_id") == window.request_id
        ]
        used_candidate_rows += len(request_candidates)
        if a_changes:
            _validate_atomic_groups(a_changes, f"A replay {window.request_id}")

        checkpoint_bids = _levels_from_json(checkpoint.get("bids"))
        checkpoint_asks = _levels_from_json(checkpoint.get("asks"))
        _validate_uncrossed_book(bids, asks, asset_id=window.asset_id)
        _validate_uncrossed_book(
            checkpoint_bids, checkpoint_asks, asset_id=window.asset_id
        )
        before_hash = _state_hash(window.asset_id, bids, asks)
        groups = [
            *_atomic_groups(a_changes),
            *_atomic_groups(request_candidates),
        ]
        _reject_ambiguous_cross_source_order(groups, window.request_id)
        groups.sort(key=_group_order_key)
        for group in groups:
            try:
                apply_price_change_group_with_top_fence(
                    bids,
                    asks,
                    group,
                    asset_id=window.asset_id,
                    require_complete_top_hints=True,
                )
            except L2ReplayNotReady as exc:
                raise ThreeSourceGoldCommitError(
                    f"request {window.request_id} replay failed: {exc}"
                ) from exc
        _validate_uncrossed_book(bids, asks, asset_id=window.asset_id)
        replay_hash = _state_hash(window.asset_id, bids, asks)
        checkpoint_hash = _state_hash(window.asset_id, checkpoint_bids, checkpoint_asks)
        if bids != checkpoint_bids or asks != checkpoint_asks:
            raise ThreeSourceGoldCommitError(
                f"request {window.request_id} A+candidate state mismatches post-A checkpoint"
            )
        request_receipt = {
                "request_id": window.request_id,
                "asset_id": window.asset_id,
                "gap_start": window.gap_start.isoformat(),
                "recovered_at": window.recovered_at.isoformat(),
                "pre_checkpoint_received_at": baseline_received.isoformat(),
                "post_checkpoint_received_at": checkpoint_received.isoformat(),
                "post_checkpoint_delay_seconds": delay,
                "source_a_price_change_rows": len(a_changes),
                "candidate_price_change_rows": len(request_candidates),
                "pre_state_sha256": before_hash,
                "replayed_state_sha256": replay_hash,
                "post_a_checkpoint_state_sha256": checkpoint_hash,
                "status": "PASS",
                "pre_state_mode": (
                    CONSENSUS_PRE_GAP_STATE
                    if use_consensus
                    else "SOURCE_A_PRE_GAP_BOOK"
                ),
            }
        if mode == CONTINUITY_ONLY_MODE:
            if before_hash != checkpoint_hash or request_candidates:
                raise ThreeSourceGoldCommitError(
                    f"request {window.request_id} continuity-only A state mismatches"
                )
            request_receipt.update(
                {
                    "mode": CONTINUITY_ONLY_MODE,
                    "source_a_price_change_rows_in_gap": len(a_gap_changes),
                    "coverage_repair_proven": True,
                    "canonical_repair_applied": False,
                }
            )
        receipts.append(request_receipt)
    if used_candidate_rows != len(candidate_rows):
        raise ThreeSourceGoldCommitError(
            "not every candidate row participated in exactly one state replay"
        )
    result = {
        "schema_version": STATE_REPLAY_SCHEMA,
        "status": "PASS",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "request_sha256": request_sha,
        "candidate_sha256": candidate_sha,
        "source_a_state_manifest_sha256": state_manifest_sha,
        "request_count": len(windows),
        "candidate_row_count": len(candidate_rows),
        "ordering_contract": "EXCHANGE_TS_WITH_CONFLICT_FAIL_CLOSED",
        "requests": receipts,
    }
    if mode == CONTINUITY_ONLY_MODE:
        result.update(
            {
                "mode": CONTINUITY_ONLY_MODE,
                "coverage_repair_proven": True,
                "canonical_repair_applied": False,
            }
        )
    return result


def _validate_consensus_manifest_state(
    payload: Mapping[str, Any], asset_id: str
) -> None:
    sources = payload.get("sources")
    if (
        payload.get("mode") != CONSENSUS_PRE_GAP_STATE
        or not asset_id
        or not _valid_sha(payload.get("state_sha256"))
        or not isinstance(payload.get("bids"), list)
        or not isinstance(payload.get("asks"), list)
        or not isinstance(sources, dict)
        or set(sources) != {"C", "D"}
    ):
        raise ThreeSourceGoldCommitError("C/D consensus pre-gap state is malformed")
    bids = _levels_from_json(json.dumps(payload["bids"]))
    asks = _levels_from_json(json.dumps(payload["asks"]))
    state_at = _parse_datetime(payload.get("state_at"))
    _validate_uncrossed_book(bids, asks, asset_id=asset_id)
    state_sha = _state_hash(asset_id, bids, asks)
    if state_sha != payload.get("state_sha256"):
        raise ThreeSourceGoldCommitError("C/D consensus pre-gap state SHA mismatch")
    for source_id in ("C", "D"):
        row = sources.get(source_id)
        anchor = row.get("anchor") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or not str(row.get("source") or "").strip()
            or not isinstance(anchor, dict)
            or anchor.get("source") != row.get("source")
            or _parse_datetime(anchor.get("timestamp_received")) >= state_at
            or not _is_one(row.get("anchor_to_gap_continuity"))
            or int(row.get("reconstruction_event_count", -1)) < 0
            or not _valid_sha(row.get("reconstruction_identity_sha256"))
            or row.get("state_sha256") != state_sha
            or row.get("bids") != payload.get("bids")
            or row.get("asks") != payload.get("asks")
        ):
            raise ThreeSourceGoldCommitError(
                f"C/D consensus pre-gap state has invalid {source_id} provenance"
            )


def _select_manifest_checkpoint(
    candidates: Sequence[Mapping[str, Any]],
    declared: Any,
    request_id: str,
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(declared, dict):
        raise ThreeSourceGoldCommitError(
            f"request {request_id} lacks manifest-selected {label} checkpoint"
        )
    identity_fields = (
        "source",
        "raw_connection_id",
        "raw_connection_generation",
        "raw_frame_seq",
        "message_index",
        "change_index",
        "group_id",
    )
    matches = [
        row
        for row in candidates
        if _parse_datetime(row.get("timestamp"))
        == _parse_datetime(declared.get("timestamp"))
        and _parse_datetime(row.get("timestamp_received"))
        == _parse_datetime(declared.get("timestamp_received"))
        and all(str(row.get(name)) == str(declared.get(name)) for name in identity_fields)
    ]
    if len(matches) != 1:
        raise ThreeSourceGoldCommitError(
            f"request {request_id} manifest-selected {label} checkpoint is not unique"
        )
    return matches[0]


def _validate_state_replay_receipt(
    payload: Mapping[str, Any],
    *,
    request_sha: str,
    candidate_sha: str,
    state_manifest_sha: str,
    request_ids: Sequence[str],
    candidate_row_count: int,
    mode: str,
) -> None:
    rows = payload.get("requests")
    if (
        payload.get("schema_version") != STATE_REPLAY_SCHEMA
        or payload.get("status") != "PASS"
        or payload.get("request_sha256") != request_sha
        or payload.get("candidate_sha256") != candidate_sha
        or payload.get("source_a_state_manifest_sha256") != state_manifest_sha
        or int(payload.get("request_count", -1)) != len(request_ids)
        or int(payload.get("candidate_row_count", -1)) != candidate_row_count
        or not isinstance(rows, list)
        or sorted(str(row.get("request_id")) for row in rows) != sorted(request_ids)
        or sum(int(row.get("candidate_price_change_rows", -1)) for row in rows)
        != candidate_row_count
        or any(
            row.get("status") != "PASS"
            or row.get("replayed_state_sha256")
            != row.get("post_a_checkpoint_state_sha256")
            for row in rows
        )
    ):
        raise ThreeSourceGoldCommitError(
            "generated state replay receipt failed semantic validation"
        )
    assert isinstance(rows, list)
    if mode == CONTINUITY_ONLY_MODE and (
        payload.get("mode") != CONTINUITY_ONLY_MODE
        or payload.get("coverage_repair_proven") is not True
        or payload.get("canonical_repair_applied") is not False
        or candidate_row_count != 0
        or any(
            row.get("mode") != CONTINUITY_ONLY_MODE
            or int(row.get("candidate_price_change_rows", -1)) != 0
            or int(row.get("source_a_price_change_rows_in_gap", -1)) != 0
            or row.get("pre_state_sha256")
            != row.get("post_a_checkpoint_state_sha256")
            or row.get("coverage_repair_proven") is not True
            or row.get("canonical_repair_applied") is not False
            for row in rows
        )
    ):
        raise ThreeSourceGoldCommitError(
            "continuity-only state replay receipt is not a zero-event state proof"
        )


def _write_overlay_atomic(
    *,
    candidate_path: Path,
    windows: Sequence[_RequestWindow],
    output_path: Path,
    hour: datetime,
    idempotency_key: str,
    candidate_sha: str,
    state_receipt_sha: str,
    memory_limit: str,
) -> dict[str, Any]:
    temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET memory_limit='{_sql(memory_limit)}'")
        connection.execute("SET threads=2")
        connection.execute(
            """
            CREATE TEMP TABLE request_windows(
                request_id VARCHAR,
                asset_id VARCHAR,
                start_ms BIGINT,
                end_ms BIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO request_windows VALUES (?, ?, ?, ?)",
            [
                (
                    row.request_id,
                    row.asset_id,
                    int(row.gap_start.timestamp() * 1000),
                    int(row.recovered_at.timestamp() * 1000),
                )
                for row in windows
            ],
        )
        columns = {
            str(row[0])
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{_sql(str(candidate_path))}')"
            ).fetchall()
        }
        replaced = {
            "canonical_candidate_source",
            "canonical_source",
            "source_mask",
            "repair_reason",
            "repair_status",
            "evidence_level",
            "request_id",
            "candidate_sha256",
            "idempotency_key",
            "state_replay_receipt_sha256",
        }.intersection(columns)
        projection = "c.*"
        if replaced:
            projection += (
                " EXCLUDE (" + ", ".join(f'"{name}"' for name in sorted(replaced)) + ")"
            )
        query = f"""
            SELECT
                {projection},
                'C'::VARCHAR AS canonical_source,
                24::INTEGER AS source_mask,
                'C_D_quorum_repairs_A'::VARCHAR AS repair_reason,
                'EVENT_REPAIRED_QUORUM'::VARCHAR AS repair_status,
                2::INTEGER AS evidence_level,
                w.request_id,
                '{candidate_sha}'::VARCHAR AS candidate_sha256,
                '{idempotency_key}'::VARCHAR AS idempotency_key,
                '{state_receipt_sha}'::VARCHAR
                    AS state_replay_receipt_sha256
            FROM read_parquet('{_sql(str(candidate_path))}') c
            JOIN request_windows w
              ON cast(c.asset_id AS VARCHAR) = w.asset_id
             AND epoch_ms(c.timestamp_received) >= w.start_ms
             AND epoch_ms(c.timestamp_received) < w.end_ms
            ORDER BY
                c.asset_id,
                c.timestamp,
                c.occurrence_rank,
                c.raw_frame_seq,
                c.message_index,
                c.change_index
        """
        connection.execute(
            f"""
            COPY ({query}) TO '{_sql(str(temporary))}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9)
            """
        )
        _fsync_file(temporary)
        summary = connection.execute(
            f"""
            SELECT count(*)::BIGINT, count(DISTINCT asset_id)::BIGINT,
                   count(*) FILTER (
                       WHERE canonical_source != 'C'
                          OR source_mask != 24
                          OR repair_status != 'EVENT_REPAIRED_QUORUM'
                          OR evidence_level != 2
                          OR candidate_sha256 != ?
                          OR idempotency_key != ?
                          OR state_replay_receipt_sha256 != ?
                   )::BIGINT
            FROM read_parquet('{_sql(str(temporary))}')
            """,
            [candidate_sha, idempotency_key, state_receipt_sha],
        ).fetchone()
        if not summary or int(summary[0]) <= 0 or int(summary[2]) != 0:
            raise ThreeSourceGoldCommitError(
                "materialized overlay failed provenance validation"
            )
        invariants = validate_gold_hour(
            path=temporary,
            hour_start=hour,
            memory_limit=memory_limit,
        )
        if invariants.get("status") != "PASS":
            raise ThreeSourceGoldCommitError(
                "materialized overlay failed Gold invariants: "
                + json.dumps(invariants, sort_keys=True)
            )
        os.replace(temporary, output_path)
        _fsync_directory(output_path.parent)
        return {
            "path": str(output_path),
            "sha256": _sha256_file(output_path),
            "bytes": output_path.stat().st_size,
            "row_count": int(summary[0]),
            "asset_count": int(summary[1]),
            "invariants": invariants,
        }
    except (duckdb.Error, OSError, ValueError, L2ReplayNotReady) as exc:
        raise ThreeSourceGoldCommitError(
            f"Gold overlay materialization failed: {exc}"
        ) from exc
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)


def _write_continuity_overlay_atomic(
    *,
    schema_paths: Sequence[Path],
    output_path: Path,
    hour: datetime,
    idempotency_key: str,
    candidate_sha: str,
    state_receipt_sha: str,
    memory_limit: str,
) -> dict[str, Any]:
    """Write a schema-complete empty overlay for a proven quiet gap."""

    if not schema_paths:
        raise ThreeSourceGoldCommitError(
            "continuity-only overlay lacks a Source-A schema input"
        )
    temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    path_list = "[" + ",".join(
        f"'{_sql(str(path))}'" for path in schema_paths
    ) + "]"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET memory_limit='{_sql(memory_limit)}'")
        connection.execute("SET threads=2")
        columns = {
            str(row[0])
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet({path_list}, union_by_name=true)"
            ).fetchall()
        }
        replaced = {
            "occurrence_rank",
            "event_time_match_delta_ms",
            "canonical_candidate_source",
            "canonical_source",
            "source_mask",
            "repair_reason",
            "repair_status",
            "evidence_level",
            "request_id",
            "candidate_sha256",
            "idempotency_key",
            "state_replay_receipt_sha256",
        }.intersection(columns)
        projection = "s.*"
        if replaced:
            projection += (
                " EXCLUDE ("
                + ", ".join(f'"{name}"' for name in sorted(replaced))
                + ")"
            )
        query = f"""
            SELECT
                {projection},
                NULL::BIGINT AS occurrence_rank,
                NULL::BIGINT AS event_time_match_delta_ms,
                'C'::VARCHAR AS canonical_candidate_source,
                'C'::VARCHAR AS canonical_source,
                24::INTEGER AS source_mask,
                'C_D_CONTINUITY_PROVES_NO_EVENTS'::VARCHAR AS repair_reason,
                '{CONTINUITY_ONLY_MODE}'::VARCHAR AS repair_status,
                2::INTEGER AS evidence_level,
                NULL::VARCHAR AS request_id,
                '{candidate_sha}'::VARCHAR AS candidate_sha256,
                '{idempotency_key}'::VARCHAR AS idempotency_key,
                '{state_receipt_sha}'::VARCHAR AS state_replay_receipt_sha256
            FROM read_parquet({path_list}, union_by_name=true) s
            WHERE FALSE
        """
        connection.execute(
            f"""
            COPY ({query}) TO '{_sql(str(temporary))}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9)
            """
        )
        _fsync_file(temporary)
        columns = {
            str(row[0])
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{_sql(str(temporary))}')"
            ).fetchall()
        }
        required = {
            "event_type",
            "market",
            "asset_id",
            "timestamp",
            "timestamp_received",
            "timestamp_normalized",
            "canonical_source",
            "source_mask",
            "repair_status",
            "evidence_level",
            "request_id",
            "candidate_sha256",
            "idempotency_key",
            "state_replay_receipt_sha256",
        }
        row_count = int(
            connection.execute(
                f"SELECT count(*) FROM read_parquet('{_sql(str(temporary))}')"
            ).fetchone()[0]
        )
        if row_count != 0 or not required.issubset(columns):
            raise ThreeSourceGoldCommitError(
                "materialized continuity-only overlay is not empty/schema-complete"
            )
        os.replace(temporary, output_path)
        _fsync_directory(output_path.parent)
        return {
            "path": str(output_path),
            "sha256": _sha256_file(output_path),
            "bytes": output_path.stat().st_size,
            "row_count": 0,
            "asset_count": 0,
            "invariants": {
                "schema_version": (
                    "polymarket-l2-continuity-only-overlay-invariants-v1"
                ),
                "hour_start": hour.isoformat(),
                "status": "PASS",
                "rows": 0,
                "schema_complete": True,
                "coverage_repair_proven": True,
            },
        }
    except ThreeSourceGoldCommitError:
        raise
    except (duckdb.Error, OSError, TypeError, ValueError) as exc:
        raise ThreeSourceGoldCommitError(
            f"continuity-only overlay materialization failed: {exc}"
        ) from exc
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)


def _manifest_artifact_path(raw: Any, manifest_path: Path, label: str) -> Path:
    text = str(raw or "").strip()
    if not text:
        raise ThreeSourceGoldCommitError(f"Gold {label} path is empty")
    value = Path(text)
    candidates = (
        [value] if value.is_absolute() else [manifest_path.parent / value, value]
    )
    resolved: list[Path] = []
    for candidate in candidates:
        if candidate.is_symlink():
            raise ThreeSourceGoldCommitError(
                f"Gold {label} must be a regular file, not a symlink: {candidate}"
            )
        try:
            path = candidate.resolve(strict=True)
        except OSError:
            continue
        if path not in resolved:
            resolved.append(path)
    if len(resolved) != 1:
        raise ThreeSourceGoldCommitError(
            f"Gold {label} path is unavailable or ambiguous"
        )
    _require_regular_file(resolved[0], f"Gold {label}")
    return resolved[0]


def _validate_state_receipt_windows(
    *,
    state: Mapping[str, Any],
    windows: Sequence[_RequestWindow],
    mode: str,
) -> None:
    rows = state.get("requests")
    if not isinstance(rows, list):
        raise ThreeSourceGoldCommitError("Gold state receipt has no request rows")
    by_id = {str(row.get("request_id")): row for row in rows if isinstance(row, dict)}
    if set(by_id) != {window.request_id for window in windows}:
        raise ThreeSourceGoldCommitError(
            "Gold state receipt request set mismatches repair windows"
        )
    for window in windows:
        row = by_id[window.request_id]
        if (
            row.get("status") != "PASS"
            or str(row.get("asset_id") or "") != window.asset_id
            or _parse_datetime(row.get("gap_start")) != window.gap_start
            or _parse_datetime(row.get("recovered_at")) != window.recovered_at
            or _parse_datetime(row.get("pre_checkpoint_received_at")) > window.gap_start
            or _parse_datetime(row.get("post_checkpoint_received_at"))
            < window.recovered_at
            or int(row.get("source_a_price_change_rows", -1)) < 0
            or int(row.get("candidate_price_change_rows", -1)) < 0
            or not _valid_sha(row.get("pre_state_sha256"))
            or not _valid_sha(row.get("replayed_state_sha256"))
            or row.get("replayed_state_sha256")
            != row.get("post_a_checkpoint_state_sha256")
        ):
            raise ThreeSourceGoldCommitError(
                f"Gold state receipt is invalid for request {window.request_id}"
            )
        if mode == CONTINUITY_ONLY_MODE and (
            row.get("mode") != CONTINUITY_ONLY_MODE
            or int(row.get("source_a_price_change_rows_in_gap", -1)) != 0
            or int(row.get("candidate_price_change_rows", -1)) != 0
            or row.get("pre_state_sha256")
            != row.get("post_a_checkpoint_state_sha256")
            or row.get("coverage_repair_proven") is not True
            or row.get("canonical_repair_applied") is not False
        ):
            raise ThreeSourceGoldCommitError(
                f"Gold continuity-only state is invalid for request {window.request_id}"
            )


def _validate_committed_overlay(
    *,
    path: Path,
    hour: datetime,
    windows: Sequence[_RequestWindow],
    candidate_sha: str,
    idempotency_key: str,
    state_receipt_sha: str,
    declared: Mapping[str, Any],
    memory_limit: str,
    mode: str,
) -> dict[str, Any]:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET memory_limit='{_sql(memory_limit)}'")
        connection.execute("SET threads=2")
        if mode == CONTINUITY_ONLY_MODE:
            columns = {
                str(row[0])
                for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{_sql(str(path))}')"
                ).fetchall()
            }
            required = {
                "event_type",
                "market",
                "asset_id",
                "timestamp",
                "timestamp_received",
                "timestamp_normalized",
                "source",
                "bids",
                "asks",
                "price",
                "size",
                "side",
                "best_bid",
                "best_ask",
                "raw_connection_id",
                "raw_connection_generation",
                "raw_frame_seq",
                "message_index",
                "change_index",
                "group_id",
                "is_last_in_group",
                "raw_frame_complete",
                "occurrence_rank",
                "canonical_source",
                "source_mask",
                "repair_status",
                "evidence_level",
                "request_id",
                "candidate_sha256",
                "idempotency_key",
                "state_replay_receipt_sha256",
            }
            row_count = int(
                connection.execute(
                    f"SELECT count(*) FROM read_parquet('{_sql(str(path))}')"
                ).fetchone()[0]
            )
            if (
                not required.issubset(columns)
                or row_count != 0
                or int(declared.get("row_count", -1)) != 0
                or int(declared.get("asset_count", -1)) != 0
            ):
                raise ThreeSourceGoldCommitError(
                    "Gold continuity-only overlay is nonempty or schema-incomplete"
                )
            return {
                "path": str(path),
                "sha256": _sha256_file(path),
                "bytes": path.stat().st_size,
                "row_count": 0,
                "asset_count": 0,
                "invariants": {
                    "schema_version": (
                        "polymarket-l2-continuity-only-overlay-invariants-v1"
                    ),
                    "status": "PASS",
                    "rows": 0,
                    "schema_complete": True,
                    "coverage_repair_proven": True,
                },
            }
        connection.execute(
            """
            CREATE TEMP TABLE repair_windows(
                request_id VARCHAR, asset_id VARCHAR,
                gap_start TIMESTAMPTZ, recovered_at TIMESTAMPTZ
            )
            """
        )
        connection.executemany(
            "INSERT INTO repair_windows VALUES (?, ?, ?, ?)",
            [
                (
                    window.request_id,
                    window.asset_id,
                    window.gap_start,
                    window.recovered_at,
                )
                for window in windows
            ],
        )
        summary = connection.execute(
            f"""
            SELECT
                count(*)::BIGINT,
                count(DISTINCT p.asset_id)::BIGINT,
                count(*) FILTER (
                    WHERE lower(p.event_type) != 'price_change'
                       OR p.canonical_source != 'C'
                       OR p.source_mask != 24
                       OR p.repair_status != 'EVENT_REPAIRED_QUORUM'
                       OR p.evidence_level != 2
                       OR p.candidate_sha256 != ?
                       OR p.idempotency_key != ?
                       OR p.state_replay_receipt_sha256 != ?
                )::BIGINT,
                count(*) FILTER (WHERE w.request_id IS NULL)::BIGINT
            FROM read_parquet('{_sql(str(path))}') p
            LEFT JOIN repair_windows w
              ON p.request_id = w.request_id
             AND cast(p.asset_id AS VARCHAR) = w.asset_id
             AND p.timestamp_received >= w.gap_start
             AND p.timestamp_received < w.recovered_at
            """,
            [candidate_sha, idempotency_key, state_receipt_sha],
        ).fetchone()
        if (
            not summary
            or int(summary[0]) <= 0
            or int(summary[0]) != int(declared.get("row_count", -1))
            or int(summary[1]) != int(declared.get("asset_count", -1))
            or int(summary[2]) != 0
            or int(summary[3]) != 0
        ):
            raise ThreeSourceGoldCommitError(
                "Gold overlay rows violate provenance or repair-window scope"
            )
        invariants = validate_gold_hour(
            path=path, hour_start=hour, memory_limit=memory_limit
        )
        if invariants.get("status") != "PASS":
            raise ThreeSourceGoldCommitError(
                "Gold overlay fails canonical hour invariants"
            )
        return {
            "path": str(path),
            "sha256": _sha256_file(path),
            "bytes": path.stat().st_size,
            "row_count": int(summary[0]),
            "asset_count": int(summary[1]),
            "invariants": invariants,
        }
    except ThreeSourceGoldCommitError:
        raise
    except (duckdb.Error, OSError, TypeError, ValueError) as exc:
        raise ThreeSourceGoldCommitError(
            f"Gold overlay validation failed: {exc}"
        ) from exc
    finally:
        connection.close()


def _validate_existing_commit(
    *,
    manifest_path: Path,
    overlay_path: Path,
    state_receipt_path: Path,
    hour: datetime,
    idempotency_key: str,
    request_sha: str,
    candidate_sha: str,
    comparison_sha: str,
    continuity_sha: str,
    state_manifest_sha: str,
    memory_limit: str,
    mode: str,
) -> dict[str, Any]:
    manifest = _load_json(manifest_path, "existing Gold manifest")
    overlay = manifest.get("overlay")
    state = manifest.get("state_replay_receipt")
    if (
        manifest.get("schema_version") != OVERLAY_SCHEMA
        or manifest.get("status") != "PASS"
        or manifest.get("idempotency_key") != idempotency_key
        or manifest.get("request_sha256") != request_sha
        or manifest.get("candidate_sha256") != candidate_sha
        or manifest.get("comparison_sha256") != comparison_sha
        or manifest.get("continuity_proof_sha256") != continuity_sha
        or manifest.get("source_a_state_manifest_sha256") != state_manifest_sha
        or str(manifest.get("mode") or EVENT_REPAIR_MODE) != mode
        or not isinstance(overlay, dict)
        or not isinstance(state, dict)
    ):
        raise ThreeSourceGoldCommitError("existing Gold manifest input mismatch")
    if _manifest_artifact_path(
        overlay.get("path"), manifest_path, "overlay"
    ) != overlay_path.resolve(strict=True) or _manifest_artifact_path(
        state.get("path"), manifest_path, "state replay receipt"
    ) != state_receipt_path.resolve(strict=True):
        raise ThreeSourceGoldCommitError("existing Gold artifact path mismatch")
    _require_regular_file(overlay_path, "existing overlay")
    _require_regular_file(state_receipt_path, "existing state replay receipt")
    if (
        _sha256_file(overlay_path) != overlay.get("sha256")
        or overlay_path.stat().st_size != int(overlay.get("bytes", -1))
        or _sha256_file(state_receipt_path) != state.get("sha256")
    ):
        raise ThreeSourceGoldCommitError("existing Gold artifact SHA/size mismatch")
    validate_three_source_gold_manifest(
        manifest_path=manifest_path,
        memory_limit=memory_limit,
    )
    result = _manifest_result_with_resolved_artifacts(
        manifest,
        overlay_path=overlay_path,
        state_receipt_path=state_receipt_path,
    )
    result["manifest_path"] = str(manifest_path)
    result["manifest_sha256"] = _sha256_file(manifest_path)
    result["idempotent_replay"] = True
    return result


def _read_parquet_rows(
    paths: Sequence[Path], *, memory_limit: str
) -> list[dict[str, Any]]:
    if not paths:
        return []
    path_list = "[" + ",".join("'" + _sql(str(path)) + "'" for path in paths) + "]"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET memory_limit='{_sql(memory_limit)}'")
        connection.execute("SET threads=2")
        cursor = connection.execute(
            f"""
            SELECT *
            FROM read_parquet({path_list}, union_by_name=true)
            ORDER BY asset_id, timestamp_received, collector_seq,
                     sequence_in_message, raw_frame_seq,
                     message_index, change_index
            """
        )
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    except duckdb.Error as exc:
        raise ThreeSourceGoldCommitError(
            f"failed to read hash-bound Parquet evidence: {exc}"
        ) from exc
    finally:
        connection.close()


def _validate_atomic_groups(rows: Sequence[Mapping[str, Any]], label: str) -> None:
    for group in _atomic_groups(rows):
        if (
            sum(bool(row.get("is_last_in_group")) for row in group) != 1
            or sum(bool(row.get("raw_frame_complete")) for row in group) != 1
        ):
            raise ThreeSourceGoldCommitError(
                f"{label} contains a truncated or non-atomic raw group"
            )


def _reject_duplicate_raw_rows(rows: Sequence[Mapping[str, Any]], label: str) -> None:
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        identity = tuple(
            str(row.get(name) if row.get(name) is not None else "")
            for name in (
                "source",
                "raw_connection_id",
                "raw_connection_generation",
                "raw_frame_seq",
                "message_index",
                "change_index",
                "group_id",
                "event_type",
                "asset_id",
            )
        )
        if identity in seen:
            raise ThreeSourceGoldCommitError(
                f"{label} contains duplicate raw event identities"
            )
        seen.add(identity)


def _atomic_groups(
    rows: Sequence[Mapping[str, Any]],
) -> list[list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str, str, str, str], list[Mapping[str, Any]]] = (
        defaultdict(list)
    )
    for row in rows:
        key_values = (
            row.get("source"),
            row.get("raw_connection_id"),
            row.get("raw_connection_generation"),
            row.get("raw_frame_seq"),
            row.get("group_id"),
        )
        if any(value is None or str(value).strip() == "" for value in key_values):
            raise ThreeSourceGoldCommitError("price-change group lacks raw identity")
        key = (
            str(key_values[0]),
            str(key_values[1]),
            str(key_values[2]),
            str(key_values[3]),
            str(key_values[4]),
        )
        grouped[key].append(row)
    return [
        sorted(
            group,
            key=lambda row: (
                int(row.get("message_index") or 0),
                int(row.get("change_index") or 0),
            ),
        )
        for _, group in sorted(grouped.items())
    ]


def _reject_ambiguous_cross_source_order(
    groups: Sequence[Sequence[Mapping[str, Any]]], request_id: str
) -> None:
    by_time: dict[datetime, list[Sequence[Mapping[str, Any]]]] = defaultdict(list)
    for group in groups:
        timestamps = {_parse_datetime(row.get("timestamp")) for row in group}
        if len(timestamps) != 1:
            raise ThreeSourceGoldCommitError(
                f"request {request_id} has multi-clock atomic group"
            )
        by_time[next(iter(timestamps))].append(group)
    for same_time in by_time.values():
        for index, left in enumerate(same_time):
            left_source = str(left[0].get("source") or "")
            left_levels = {
                (str(row.get("side") or "").upper(), str(row.get("price")))
                for row in left
            }
            for right in same_time[index + 1 :]:
                right_source = str(right[0].get("source") or "")
                right_levels = {
                    (str(row.get("side") or "").upper(), str(row.get("price")))
                    for row in right
                }
                if left_source != right_source and left_levels.intersection(
                    right_levels
                ):
                    raise ThreeSourceGoldCommitError(
                        f"request {request_id} has ambiguous cross-source event order"
                    )


def _group_order_key(
    group: Sequence[Mapping[str, Any]],
) -> tuple[Any, ...]:
    row = group[0]
    source = str(row.get("source") or "")
    return (
        _parse_datetime(row.get("timestamp")),
        0 if source.upper() == "A" or source.lower().endswith("_a") else 1,
        str(row.get("raw_connection_id")),
        int(row.get("raw_connection_generation") or 0),
        int(row.get("raw_frame_seq") or 0),
        str(row.get("group_id")),
    )


def _validate_checkpoint_row(
    row: Mapping[str, Any], request_id: str, label: str
) -> None:
    if (
        not bool(row.get("is_last_in_group"))
        or not bool(row.get("raw_frame_complete"))
        or not str(row.get("group_id") or "").strip()
        or not str(row.get("raw_connection_id") or "").strip()
        or row.get("bids") is None
        or row.get("asks") is None
    ):
        raise ThreeSourceGoldCommitError(
            f"request {request_id} {label} A checkpoint is not complete"
        )


def _request_for_row(
    row: Mapping[str, Any], windows: Sequence[_RequestWindow]
) -> str | None:
    asset_id = str(row.get("asset_id") or "")
    received = _parse_datetime(row.get("timestamp_received"))
    matches = [
        window.request_id
        for window in windows
        if window.asset_id == asset_id
        and window.gap_start <= received < window.recovered_at
    ]
    return matches[0] if len(matches) == 1 else None


def _row_order_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        _parse_datetime(row.get("timestamp_received")),
        int(row.get("collector_seq") or 0),
        int(row.get("sequence_in_message") or 0),
        int(row.get("raw_frame_seq") or 0),
        int(row.get("message_index") or 0),
        int(row.get("change_index") or 0),
    )


def _state_hash(
    asset_id: str,
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
) -> str:
    payload = {
        "asset_id": asset_id,
        "bids": [
            [_decimal_text(price), _decimal_text(size)]
            for price, size in sorted(bids.items(), reverse=True)
        ],
        "asks": [
            [_decimal_text(price), _decimal_text(size)]
            for price, size in sorted(asks.items())
        ],
    }
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _idempotency_key(request_ids: Sequence[str], candidate_sha: str) -> str:
    return hashlib.sha256(
        _json_bytes(
            {
                "request_ids": sorted(str(value) for value in request_ids),
                "candidate_sha256": candidate_sha,
            }
        )
    ).hexdigest()


def _manifest_result_with_resolved_artifacts(
    manifest: Mapping[str, Any], *, overlay_path: Path, state_receipt_path: Path
) -> dict[str, Any]:
    result = dict(manifest)
    overlay = dict(result.get("overlay") or {})
    state = dict(result.get("state_replay_receipt") or {})
    overlay["path"] = str(overlay_path)
    state["path"] = str(state_receipt_path)
    result["overlay"] = overlay
    result["state_replay_receipt"] = state
    return result


def _load_sha_bound_json(
    path: Path, *, expected_sha: str, label: str
) -> dict[str, Any]:
    try:
        payload_bytes = path.read_bytes()
        payload = json.loads(payload_bytes)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ThreeSourceGoldCommitError(f"invalid {label}: {exc}") from exc
    if (
        not isinstance(payload, dict)
        or not _valid_sha(expected_sha)
        or hashlib.sha256(payload_bytes).hexdigest() != expected_sha
    ):
        raise ThreeSourceGoldCommitError(f"{label} SHA or payload is invalid")
    return payload


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ThreeSourceGoldCommitError(f"invalid {label}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ThreeSourceGoldCommitError(f"{label} must be a JSON object")
    return payload


def _require_regular_file(path: Path, label: str) -> None:
    if path.is_symlink():
        raise ThreeSourceGoldCommitError(
            f"{label} must be a regular file, not a symlink: {path}"
        )
    if not path.is_file() or path.stat().st_size <= 0:
        raise ThreeSourceGoldCommitError(f"{label} is missing or empty: {path}")


def _require_same_path(actual: Any, expected: Path, label: str) -> None:
    try:
        actual_path = Path(str(actual)).resolve(strict=True)
        expected_path = Path(expected).resolve(strict=True)
    except (OSError, ValueError) as exc:
        raise ThreeSourceGoldCommitError(f"{label} path binding is invalid") from exc
    if actual_path != expected_path:
        raise ThreeSourceGoldCommitError(f"{label} path binding mismatch")


def _valid_sha(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _is_one(value: Any) -> bool:
    try:
        return math.isclose(float(value), 1.0, rel_tol=0.0, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ThreeSourceGoldCommitError(
                f"invalid UTC timestamp: {value!r}"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _floor_hour(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _sql(value: str) -> str:
    return str(value).replace("'", "''")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Finalize a formal C/D-quorum repair as immutable Gold overlay"
    )
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--candidate-receipt", type=Path, required=True)
    parser.add_argument("--comparison-report", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--continuity-proof", type=Path, required=True)
    parser.add_argument("--source-c-continuity-receipt", type=Path, required=True)
    parser.add_argument("--source-d-continuity-receipt", type=Path, required=True)
    parser.add_argument("--source-a-completeness-proof", type=Path, required=True)
    parser.add_argument("--source-c-completeness-proof", type=Path, required=True)
    parser.add_argument("--source-d-completeness-proof", type=Path, required=True)
    parser.add_argument("--source-a-state-root", type=Path, required=True)
    parser.add_argument("--source-a-state-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--max-post-checkpoint-delay-seconds", type=int, default=300)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = finalize_three_source_gold_candidate(
            request_path=args.request,
            candidate_receipt_path=args.candidate_receipt,
            comparison_report_path=args.comparison_report,
            candidate_path=args.candidate,
            continuity_proof_path=args.continuity_proof,
            observer_continuity_receipts={
                "C": args.source_c_continuity_receipt,
                "D": args.source_d_continuity_receipt,
            },
            source_completeness_proofs={
                "A": args.source_a_completeness_proof,
                "C": args.source_c_completeness_proof,
                "D": args.source_d_completeness_proof,
            },
            source_a_state_root=args.source_a_state_root,
            source_a_state_manifest_path=args.source_a_state_manifest,
            output_dir=args.output_dir,
            memory_limit=args.memory_limit,
            max_post_checkpoint_delay_seconds=(args.max_post_checkpoint_delay_seconds),
        )
    except ThreeSourceGoldCommitError as exc:
        print(
            json.dumps(
                {
                    "schema_version": OVERLAY_SCHEMA,
                    "status": "FAIL_CLOSED",
                    "reason": str(exc),
                },
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
