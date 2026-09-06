"""Bounded local read-through cache for cold-tier L2 Parquet files."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Iterable, Sequence


CACHE_SCHEMA_VERSION = "polymarket-l2-archive-read-cache-v1"


class L2ArchiveColdTierUnavailable(RuntimeError):
    """Raised when a requested cold-tier file cannot be safely materialized."""


@dataclass(frozen=True)
class CacheMaterialization:
    files: tuple[str, ...]
    cache_hits: int
    files_materialized: int
    bytes_materialized: int
    bytes_evicted: int


def materialize_replay_files_from_env(
    files: Sequence[str],
    *,
    archive_dir: Path | str,
) -> list[str]:
    cache_dir = str(os.environ.get("BOOK_L2_REPLAY_CACHE_DIR") or "").strip()
    if not cache_dir:
        return list(files)
    result = materialize_replay_files(
        files,
        archive_dir=archive_dir,
        cache_dir=Path(cache_dir),
        cold_mount_dir=Path(
            os.environ.get(
                "BOOK_L2_XUE_LOCAL_MOUNT_DIR",
                "/data/jiahuaiyu/prediction-market-quant/lob_l2_archive_xue",
            )
        ),
        max_cache_bytes=int(os.environ.get("BOOK_L2_REPLAY_CACHE_MAX_BYTES", str(300 * 1024**3))),
        mount_health_file=Path(
            os.environ.get(
                "BOOK_L2_XUE_MOUNT_HEALTH_FILE",
                "runtime_outputs/gcp_l2_batch/xue_mount_health/latest.json",
            )
        ),
        mount_health_max_age_seconds=float(
            os.environ.get("BOOK_L2_XUE_MOUNT_HEALTH_MAX_AGE_SECONDS", "180")
        ),
        transfer_timeout_seconds=float(
            os.environ.get("BOOK_L2_REPLAY_CACHE_TRANSFER_TIMEOUT_SECONDS", "300")
        ),
        rsync_io_timeout_seconds=int(
            os.environ.get("BOOK_L2_REPLAY_CACHE_RSYNC_IO_TIMEOUT_SECONDS", "60")
        ),
    )
    return list(result.files)


def materialize_replay_files(
    files: Sequence[str],
    *,
    archive_dir: Path | str,
    cache_dir: Path | str,
    cold_mount_dir: Path | str,
    max_cache_bytes: int,
    mount_health_file: Path | str | None = None,
    mount_health_max_age_seconds: float = 180.0,
    transfer_timeout_seconds: float = 300.0,
    rsync_io_timeout_seconds: int = 60,
) -> CacheMaterialization:
    """Return local regular paths, materializing cold symlinks when necessary."""

    archive_root = Path(archive_dir).absolute()
    cache_root = Path(cache_dir).absolute()
    cold_root = Path(cold_mount_dir).absolute()
    if max_cache_bytes <= 0:
        raise ValueError("max_cache_bytes must be positive")
    cache_root.mkdir(parents=True, exist_ok=True)
    lock_path = cache_root / ".cache.lock"

    result_paths: list[str] = []
    cold: list[tuple[Path, Path, Path]] = []
    cache_hits = 0
    for raw in files:
        source = Path(raw).absolute()
        if not source.is_symlink():
            result_paths.append(str(source))
            continue
        try:
            relative = source.relative_to(archive_root)
        except ValueError as exc:
            raise ValueError(f"archive file escapes archive root: {source}") from exc
        link_target = Path(os.readlink(source))
        if not link_target.is_absolute():
            link_target = (source.parent / link_target).absolute()
        try:
            target_relative = link_target.relative_to(cold_root)
        except ValueError as exc:
            raise L2ArchiveColdTierUnavailable(
                f"FAIL_CLOSED_XUE_UNAVAILABLE: unexpected cold-tier link {source} -> {link_target}"
            ) from exc
        if target_relative != relative:
            raise L2ArchiveColdTierUnavailable(
                f"FAIL_CLOSED_XUE_UNAVAILABLE: cold-tier path mismatch for {relative}"
            )
        cached = cache_root / relative
        if _cache_entry_valid(cached, relative):
            _touch_cache_entry(cached)
            result_paths.append(str(cached))
            cache_hits += 1
            continue
        cold.append((source, relative, cached))
        result_paths.append(str(cached))

    if not cold:
        evicted = _evict_cache(cache_root, max_cache_bytes=max_cache_bytes, protected=())
        return CacheMaterialization(tuple(result_paths), cache_hits, 0, 0, evicted)

    _require_fresh_mount_health(
        mount_health_file,
        max_age_seconds=mount_health_max_age_seconds,
    )

    with _exclusive_lock(lock_path):
        pending = [item for item in cold if not _cache_entry_valid(item[2], item[1])]
        cache_hits += len(cold) - len(pending)
        if not pending:
            for _, _, cached in cold:
                _touch_cache_entry(cached)
            evicted = _evict_cache(
                cache_root,
                max_cache_bytes=max_cache_bytes,
                protected=(item[2] for item in cold),
            )
            return CacheMaterialization(tuple(result_paths), cache_hits, 0, 0, evicted)

        staging_parent = cache_root / ".staging"
        staging_parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="replay-", dir=staging_parent) as temporary:
            staging = Path(temporary)
            files_list = staging / "files.list"
            transfer_relatives: list[Path] = []
            for _, relative, _ in pending:
                transfer_relatives.append(relative)
                transfer_relatives.append(_sidecar_relative(relative))
            files_list.write_text(
                "".join(f"{item.as_posix()}\n" for item in transfer_relatives),
                encoding="utf-8",
            )
            command = [
                "rsync",
                "-a",
                "--relative",
                f"--timeout={max(1, int(rsync_io_timeout_seconds))}",
                f"--files-from={files_list}",
                f"{cold_root}/",
                f"{staging}/",
            ]
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=max(1.0, float(transfer_timeout_seconds)),
                )
            except subprocess.TimeoutExpired as exc:
                raise L2ArchiveColdTierUnavailable(
                    "FAIL_CLOSED_XUE_UNAVAILABLE: cold-tier cache transfer timed out"
                ) from exc
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "rsync failed").strip()
                raise L2ArchiveColdTierUnavailable(
                    f"FAIL_CLOSED_XUE_UNAVAILABLE: {detail[-1000:]}"
                )

            materialized_bytes = 0
            for _, relative, cached in pending:
                staged = staging / relative
                staged_sidecar = staging / _sidecar_relative(relative)
                proof = _verify_staged_file(staged, staged_sidecar, relative)
                cached.parent.mkdir(parents=True, exist_ok=True)
                cached_sidecar = cached.with_suffix(".parquet.manifest.json")
                os.replace(staged_sidecar, cached_sidecar)
                os.replace(staged, cached)
                marker = _cache_marker(cached)
                _atomic_write_json(
                    marker,
                    {
                        "schema_version": CACHE_SCHEMA_VERSION,
                        "relative_path": relative.as_posix(),
                        "file_size_bytes": proof[0],
                        "sha256": proof[1],
                        "materialized_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                materialized_bytes += proof[0]
                _touch_cache_entry(cached)

        evicted = _evict_cache(
            cache_root,
            max_cache_bytes=max_cache_bytes,
            protected=(item[2] for item in pending),
        )
    return CacheMaterialization(
        tuple(result_paths),
        cache_hits,
        len(pending),
        materialized_bytes,
        evicted,
    )


def _require_fresh_mount_health(path: Path | str | None, *, max_age_seconds: float) -> None:
    if path is None:
        return
    health_path = Path(path)
    try:
        payload = json.loads(health_path.read_text(encoding="utf-8"))
        checked = datetime.fromisoformat(str(payload["checked_at"]).replace("Z", "+00:00"))
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        raise L2ArchiveColdTierUnavailable(
            "FAIL_CLOSED_XUE_UNAVAILABLE: Xue mount health evidence is missing"
        ) from exc
    age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
    if payload.get("status") != "PASS" or age > max(0.0, float(max_age_seconds)):
        raise L2ArchiveColdTierUnavailable(
            f"FAIL_CLOSED_XUE_UNAVAILABLE: Xue mount health is {payload.get('status')} age={age:.1f}s"
        )


def _verify_staged_file(staged: Path, sidecar: Path, relative: Path) -> tuple[int, str]:
    try:
        proof = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise L2ArchiveColdTierUnavailable(
            f"FAIL_CLOSED_XUE_UNAVAILABLE: missing SHA sidecar for {relative}"
        ) from exc
    if str(proof.get("relative_path") or "") != relative.as_posix():
        raise L2ArchiveColdTierUnavailable(f"cold-tier sidecar path mismatch for {relative}")
    expected_size = int(proof.get("file_size_bytes") or -1)
    expected_sha = str(proof.get("sha256") or "")
    if expected_size < 0 or len(expected_sha) != 64:
        raise L2ArchiveColdTierUnavailable(f"cold-tier sidecar is incomplete for {relative}")
    try:
        actual_size = staged.stat().st_size
    except OSError as exc:
        raise L2ArchiveColdTierUnavailable(f"cold-tier file is missing for {relative}") from exc
    actual_sha = _sha256(staged)
    if actual_size != expected_size or actual_sha != expected_sha:
        raise L2ArchiveColdTierUnavailable(f"cold-tier SHA verification failed for {relative}")
    return actual_size, actual_sha


def _cache_entry_valid(path: Path, relative: Path) -> bool:
    marker = _cache_marker(path)
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
        return (
            payload.get("schema_version") == CACHE_SCHEMA_VERSION
            and payload.get("relative_path") == relative.as_posix()
            and path.is_file()
            and path.stat().st_size == int(payload.get("file_size_bytes") or -1)
            and path.with_suffix(".parquet.manifest.json").is_file()
        )
    except (OSError, ValueError, json.JSONDecodeError):
        return False


def _evict_cache(cache_root: Path, *, max_cache_bytes: int, protected: Iterable[Path]) -> int:
    protected_set = {item.absolute() for item in protected}
    entries = [
        item
        for item in cache_root.rglob("*.parquet")
        if ".staging" not in item.parts and item.is_file()
    ]
    total = sum(item.stat().st_size for item in entries)
    evicted = 0
    if total <= max_cache_bytes:
        return 0
    for item in sorted(entries, key=lambda path: path.stat().st_mtime_ns):
        if item.absolute() in protected_set:
            continue
        size = item.stat().st_size
        item.unlink(missing_ok=True)
        item.with_suffix(".parquet.manifest.json").unlink(missing_ok=True)
        _cache_marker(item).unlink(missing_ok=True)
        total -= size
        evicted += size
        if total <= max_cache_bytes:
            break
    return evicted


def _touch_cache_entry(path: Path) -> None:
    path.touch(exist_ok=True)
    _cache_marker(path).touch(exist_ok=True)


def _sidecar_relative(relative: Path) -> Path:
    return relative.with_suffix(".parquet.manifest.json")


def _cache_marker(path: Path) -> Path:
    return path.with_suffix(".parquet.read-cache.json")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
