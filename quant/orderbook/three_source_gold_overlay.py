"""Discover and consume strictly validated three-source Gold overlays.

The Gold directory is a separate, immutable evidence tier.  Merely placing a
Parquet file in that directory never makes it replayable: every caller goes
through :func:`validate_three_source_gold_manifest`, and coverage/replay share
the validated manifest SHA returned here.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

GOLD_MANIFEST_SCHEMA = "polymarket-l2-three-source-gold-overlay-v1"
_FORMAL_NAME = re.compile(
    r"^three_source_gold_overlay_(?P<tag>\d{8}T\d{2})_[^.]+\.manifest\.json$"
)


class ThreeSourceGoldOverlayError(RuntimeError):
    """Raised when configured Gold evidence cannot be trusted."""


@dataclass(frozen=True, slots=True)
class VerifiedThreeSourceGoldOverlay:
    manifest_path: Path
    manifest_sha256: str
    hour_start: datetime
    overlay_path: Path
    overlay_sha256: str
    overlay_row_count: int
    repair_windows: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ThreeSourceGoldManifestCandidate:
    """Untrusted lightweight discovery result; validation is still required."""

    manifest_path: Path
    manifest_sha256: str
    hour_start: datetime


@dataclass(frozen=True, slots=True)
class VerifiedThreeSourceGoldSet:
    overlays: tuple[VerifiedThreeSourceGoldOverlay, ...] = ()

    @property
    def overlay_paths(self) -> tuple[Path, ...]:
        return tuple(item.overlay_path for item in self.overlays)

    @property
    def manifest_sha256(self) -> tuple[str, ...]:
        return tuple(sorted({item.manifest_sha256 for item in self.overlays}))

    @property
    def overlay_row_count(self) -> int:
        return sum(item.overlay_row_count for item in self.overlays)

    def repair_windows_frame(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for overlay in self.overlays:
            for window in overlay.repair_windows:
                for shard_id in window["primary_shard_ids"]:
                    rows.append(
                        {
                            "request_id": window["request_id"],
                            "asset_id": window["asset_id"],
                            "hour_start": window["hour_start"],
                            "gap_at": window["gap_start"],
                            "recovered_at": window["recovered_at"],
                            "shard_id": int(shard_id),
                            "manifest_sha256": overlay.manifest_sha256,
                            "overlay_sha256": overlay.overlay_sha256,
                        }
                    )
        if not rows:
            return empty_gold_repair_windows()
        frame = pd.DataFrame(rows)
        for column in ("hour_start", "gap_at", "recovered_at"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        frame["shard_id"] = frame["shard_id"].astype("int64")
        return frame.drop_duplicates(
            ["request_id", "asset_id", "shard_id", "gap_at", "recovered_at"]
        ).reset_index(drop=True)


def empty_gold_repair_windows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "request_id": pd.Series(dtype="string"),
            "asset_id": pd.Series(dtype="string"),
            "hour_start": pd.Series(dtype="datetime64[ns, UTC]"),
            "gap_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "recovered_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "shard_id": pd.Series(dtype="int64"),
            "manifest_sha256": pd.Series(dtype="string"),
            "overlay_sha256": pd.Series(dtype="string"),
        }
    )


def load_verified_three_source_gold(
    root: Path | str | None,
    *,
    since: datetime | None,
    until: datetime | None,
    memory_limit: str = "2GB",
) -> VerifiedThreeSourceGoldSet:
    """Return every valid Gold overlay intersecting ``[since, until)``.

    An omitted root is the explicit compatibility mode.  A configured but
    unavailable root, or an invalid formal manifest for a matching hour, fails
    closed.  Non-Gold transfer receipts that happen to end in ``manifest.json``
    are ignored.
    """

    if root is None or not str(root).strip():
        return VerifiedThreeSourceGoldSet()
    path = Path(root)
    try:
        resolved_root = path.resolve(strict=True)
    except OSError as exc:
        raise ThreeSourceGoldOverlayError(
            f"configured three-source Gold root is unavailable: {path}: {exc}"
        ) from exc
    if not resolved_root.is_dir():
        raise ThreeSourceGoldOverlayError(
            f"configured three-source Gold root is not a directory: {resolved_root}"
        )
    start = _utc_or_none(since)
    stop = _utc_or_none(until)
    if start is not None and stop is not None and stop <= start:
        raise ValueError("three-source Gold interval must be positive")

    try:
        candidates = sorted(resolved_root.rglob("*.manifest.json"))
    except OSError as exc:
        raise ThreeSourceGoldOverlayError(
            f"cannot enumerate three-source Gold root: {resolved_root}: {exc}"
        ) from exc

    selected: list[tuple[Path, dict[str, Any], datetime]] = []
    for candidate in candidates:
        filename_hour, formal_filename = _formal_filename_hour(candidate)
        # Gold files may be tiered independently after the local hot window.
        # Classify a formal filename before touching the file so an unrelated
        # old, stale symlink cannot block coverage for the requested hour.
        if (
            formal_filename
            and filename_hour is not None
            and not _hour_intersects(filename_hour, since=start, until=stop)
        ):
            continue
        if _is_hidden_state_path(candidate, resolved_root):
            continue
        if formal_filename and candidate.is_symlink():
            raise ThreeSourceGoldOverlayError(
                f"matching formal Gold manifest is a symlink: {candidate}"
            )
        payload, discovered_hour, formal = _inspect_candidate(candidate)
        if not formal:
            continue
        if discovered_hour is None:
            # A formal Gold filename/schema with no trustworthy hour cannot be
            # classified as unrelated to this request.
            raise ThreeSourceGoldOverlayError(
                f"formal Gold manifest has no valid hour_start: {candidate}"
            )
        if not _hour_intersects(discovered_hour, since=start, until=stop):
            continue
        if payload is None:
            raise ThreeSourceGoldOverlayError(
                f"matching Gold manifest is not valid JSON: {candidate}"
            )
        selected.append((candidate, payload, discovered_hour))

    overlays: list[VerifiedThreeSourceGoldOverlay] = []
    request_owners: dict[str, str] = {}
    for manifest_path, _payload, discovered_hour in selected:
        try:
            # Delayed import avoids a module cycle: the Gold finalizer uses the
            # replay state machine, while replay also consumes this registry.
            from .three_source_gold_commit import (
                validate_three_source_gold_manifest,
            )

            validated = validate_three_source_gold_manifest(
                manifest_path=manifest_path,
                memory_limit=memory_limit,
            )
        except Exception as exc:
            raise ThreeSourceGoldOverlayError(
                f"matching three-source Gold manifest failed validation: "
                f"{manifest_path}: {exc}"
            ) from exc
        if validated.get("status") != "PASS":
            raise ThreeSourceGoldOverlayError(
                f"matching three-source Gold manifest is not PASS: {manifest_path}"
            )
        hour = _parse_datetime(validated.get("hour_start"))
        if hour != discovered_hour:
            raise ThreeSourceGoldOverlayError(
                f"validated Gold hour changed during discovery: {manifest_path}"
            )
        manifest_sha = str(validated.get("manifest_sha256") or "")
        overlay = validated.get("overlay")
        windows = validated.get("repair_windows")
        overlay_path = Path(str(validated.get("resolved_overlay_path") or ""))
        if (
            len(manifest_sha) != 64
            or not isinstance(overlay, dict)
            or len(str(overlay.get("sha256") or "")) != 64
            or int(overlay.get("row_count", -1)) < 0
            or not isinstance(windows, list)
            or not overlay_path.is_file()
        ):
            raise ThreeSourceGoldOverlayError(
                f"strict Gold validator returned an incomplete contract: {manifest_path}"
            )
        portable_windows = tuple(_normalize_window(row, hour) for row in windows)
        for window in portable_windows:
            request_id = window["request_id"]
            previous = request_owners.get(request_id)
            if previous is not None and previous != manifest_sha:
                raise ThreeSourceGoldOverlayError(
                    f"Gold request_id is owned by conflicting manifests: {request_id}"
                )
            request_owners[request_id] = manifest_sha
        overlays.append(
            VerifiedThreeSourceGoldOverlay(
                manifest_path=manifest_path.resolve(strict=True),
                manifest_sha256=manifest_sha,
                hour_start=hour,
                overlay_path=overlay_path.resolve(strict=True),
                overlay_sha256=str(overlay["sha256"]),
                overlay_row_count=int(overlay["row_count"]),
                repair_windows=portable_windows,
            )
        )

    # Identical transferred copies are harmless; apply each immutable manifest
    # SHA once so replay cannot double-count them.
    unique: dict[str, VerifiedThreeSourceGoldOverlay] = {}
    for overlay in overlays:
        existing = unique.get(overlay.manifest_sha256)
        if existing is None:
            unique[overlay.manifest_sha256] = overlay
        elif (
            existing.overlay_sha256 != overlay.overlay_sha256
            or existing.repair_windows != overlay.repair_windows
        ):
            raise ThreeSourceGoldOverlayError(
                "the same Gold manifest SHA resolved to conflicting artifacts"
            )
    return VerifiedThreeSourceGoldSet(
        tuple(
            sorted(
                unique.values(),
                key=lambda item: (item.hour_start, item.manifest_sha256),
            )
        )
    )


def discover_three_source_gold_manifests(
    root: Path | str | None,
    *,
    since: datetime | None,
    until: datetime | None,
) -> tuple[ThreeSourceGoldManifestCandidate, ...]:
    """Discover matching formal manifests without opening overlay payloads.

    This is only a scheduler hint.  Consumers must still call
    :func:`load_verified_three_source_gold` for the chosen hour before use.
    """

    if root is None or not str(root).strip():
        return ()
    path = Path(root)
    try:
        resolved_root = path.resolve(strict=True)
    except OSError as exc:
        raise ThreeSourceGoldOverlayError(
            f"configured three-source Gold root is unavailable: {path}: {exc}"
        ) from exc
    if not resolved_root.is_dir():
        raise ThreeSourceGoldOverlayError(
            f"configured three-source Gold root is not a directory: {resolved_root}"
        )
    start = _utc_or_none(since)
    stop = _utc_or_none(until)
    discovered: list[ThreeSourceGoldManifestCandidate] = []
    try:
        candidates = sorted(resolved_root.rglob("*.manifest.json"))
    except OSError as exc:
        raise ThreeSourceGoldOverlayError(
            f"cannot enumerate three-source Gold root: {resolved_root}: {exc}"
        ) from exc
    for candidate in candidates:
        filename_hour, formal_filename = _formal_filename_hour(candidate)
        if (
            formal_filename
            and filename_hour is not None
            and not _hour_intersects(filename_hour, since=start, until=stop)
        ):
            continue
        if _is_hidden_state_path(candidate, resolved_root):
            continue
        if formal_filename and candidate.is_symlink():
            raise ThreeSourceGoldOverlayError(
                f"matching formal Gold manifest is a symlink: {candidate}"
            )
        payload, hour, formal = _inspect_candidate(candidate)
        if not formal:
            continue
        if hour is None:
            raise ThreeSourceGoldOverlayError(
                f"formal Gold manifest has no valid hour_start: {candidate}"
            )
        if not _hour_intersects(hour, since=start, until=stop):
            continue
        if payload is None:
            raise ThreeSourceGoldOverlayError(
                f"matching Gold manifest is not valid JSON: {candidate}"
            )
        try:
            manifest_sha = hashlib.sha256(candidate.read_bytes()).hexdigest()
        except OSError as exc:
            raise ThreeSourceGoldOverlayError(
                f"matching Gold manifest is unreadable: {candidate}: {exc}"
            ) from exc
        discovered.append(
            ThreeSourceGoldManifestCandidate(
                manifest_path=candidate.resolve(strict=True),
                manifest_sha256=manifest_sha,
                hour_start=hour,
            )
        )
    return tuple(
        sorted(
            discovered,
            key=lambda item: (item.hour_start, item.manifest_sha256),
        )
    )


def _is_hidden_state_path(path: Path, root: Path) -> bool:
    """Exclude transfer/incoming state that is not atomically published yet."""

    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ThreeSourceGoldOverlayError(
            f"Gold discovery escaped its configured root: {path}"
        ) from exc
    return any(part.startswith(".") for part in relative.parts)


def dedupe_gold_overlay_rows(
    rows: Any,
    *,
    gold_overlay_paths: set[str] | frozenset[str],
) -> Any:
    """Deduplicate only identities in which a trusted Gold row participates.

    Existing base-only duplicates are deliberately preserved for compatibility.
    Payload hashes are feed-independent; the raw tuple is the fallback for old
    rows that predate payload hashing.  The earliest observation wins; base A
    wins an exact receive-time tie because the overlay only fills an A absence.
    """

    if rows.empty or not gold_overlay_paths or "filename" not in rows:
        return rows
    result = rows.copy()
    trusted = {str(Path(value).resolve()) for value in gold_overlay_paths}
    result["_gold_overlay_row"] = [
        str(Path(str(value)).resolve()) in trusted for value in result["filename"]
    ]
    if not bool(result["_gold_overlay_row"].any()):
        return result.drop(columns=["_gold_overlay_row"])
    records = result.to_dict("records")
    result["_gold_identity"] = [
        _event_identity(row, position) for position, row in enumerate(records)
    ]
    participating = set(
        result.loc[result["_gold_overlay_row"], "_gold_identity"].tolist()
    )
    keep = pd.Series(True, index=result.index)
    for identity in participating:
        matches = result[result["_gold_identity"] == identity]
        if len(matches) <= 1:
            continue
        ordered = matches.sort_values(
            [
                "timestamp_received",
                "_gold_overlay_row",
                "collector_seq",
                "sequence_in_message",
            ],
            kind="stable",
            na_position="last",
        )
        keep.loc[matches.index] = False
        keep.loc[ordered.index[0]] = True
    return (
        result.loc[keep]
        .drop(columns=["_gold_overlay_row", "_gold_identity"])
        .reset_index(drop=True)
    )


def _event_identity(row: dict[str, Any], position: int) -> tuple[str, ...]:
    payload_hash = _text(row.get("payload_hash"))
    asset_id = _text(row.get("asset_id"))
    event_type = _text(row.get("event_type"))
    message_index = _text(row.get("message_index"))
    change_index = _text(row.get("change_index"))
    if len(payload_hash) == 64:
        return (
            "payload",
            payload_hash,
            asset_id,
            event_type,
            message_index,
            change_index,
        )
    connection_id = _text(row.get("raw_connection_id"))
    frame_seq = _text(row.get("raw_frame_seq"))
    if connection_id and frame_seq:
        return (
            "raw",
            _text(row.get("source")),
            connection_id,
            _text(row.get("raw_connection_generation")),
            frame_seq,
            message_index,
            change_index,
            _text(row.get("group_id")),
            asset_id,
            event_type,
        )
    # Strictly validated current Gold files carry both identities.  Do not risk
    # collapsing legacy rows when neither is present.
    return ("unique", str(position))


def _text(value: Any) -> str:
    if value is None or type(value).__name__ in {"NAType", "NaTType"}:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none", "<na>"} else text


def _inspect_candidate(
    path: Path,
) -> tuple[dict[str, Any] | None, datetime | None, bool]:
    name_hour, formal_filename = _formal_filename_hour(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, name_hour, formal_filename
    if not isinstance(payload, dict):
        return None, name_hour, formal_filename
    schema_is_gold = payload.get("schema_version") == GOLD_MANIFEST_SCHEMA
    if not schema_is_gold and not formal_filename:
        return payload, None, False
    payload_hour = None
    try:
        payload_hour = _parse_datetime(payload.get("hour_start"))
    except (TypeError, ValueError):
        pass
    if name_hour is not None and payload_hour is not None and name_hour != payload_hour:
        # Keep the filename hour for matching, then let the strict validator
        # reject the inconsistent contract.
        return payload, name_hour, True
    return payload, payload_hour or name_hour, True


def _formal_filename_hour(path: Path) -> tuple[datetime | None, bool]:
    """Parse a formal Gold hour using only the lexical filename."""

    name_match = _FORMAL_NAME.match(path.name)
    if name_match is None:
        return None, False
    try:
        return (
            datetime.strptime(name_match.group("tag"), "%Y%m%dT%H").replace(
                tzinfo=timezone.utc
            ),
            True,
        )
    except ValueError:
        return None, True


def _normalize_window(row: Any, hour: datetime) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ThreeSourceGoldOverlayError(
            "validated Gold repair window is not an object"
        )
    request_id = str(row.get("request_id") or "").strip()
    asset_id = str(row.get("asset_id") or "").strip()
    window_hour = _parse_datetime(row.get("hour_start"))
    gap_start = _parse_datetime(row.get("gap_start"))
    recovered_at = _parse_datetime(row.get("recovered_at"))
    shards = row.get("primary_shard_ids")
    if (
        not request_id
        or not asset_id
        or window_hour != hour
        or not (hour <= gap_start < recovered_at <= hour + timedelta(hours=1))
        or not isinstance(shards, list)
        or not shards
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value >= 48
            for value in shards
        )
    ):
        raise ThreeSourceGoldOverlayError(
            f"strict Gold validator returned an invalid repair window: {request_id}"
        )
    return {
        "request_id": request_id,
        "asset_id": asset_id,
        "hour_start": hour,
        "gap_start": gap_start,
        "recovered_at": recovered_at,
        "primary_shard_ids": tuple(sorted(set(shards))),
    }


def _hour_intersects(
    hour: datetime,
    *,
    since: datetime | None,
    until: datetime | None,
) -> bool:
    return (until is None or hour < until) and (
        since is None or hour + timedelta(hours=1) > since
    )


def _parse_datetime(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_or_none(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return (
        value.astimezone(timezone.utc)
        if value.tzinfo
        else value.replace(tzinfo=timezone.utc)
    )
