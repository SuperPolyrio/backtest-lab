"""Bounded, recoverable Parquet writer for replay results."""

from __future__ import annotations

import hashlib
import json
import os
import queue
import threading
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self

import pyarrow as pa
import pyarrow.parquet as pq

WriterMode = Literal["research", "audit"]
WRITER_SCHEMA_VERSION = "UnifiedFillOnlyResultWriterV1"
RESULT_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("profile", pa.string(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("filled_size", pa.string(), nullable=False),
        pa.field("avg_price", pa.string(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("evidence_tier", pa.string()),
        pa.field("result_role", pa.string()),
        pa.field("is_modeled", pa.bool_(), nullable=False),
        pa.field("result_json", pa.string()),
        pa.field("source_fills_json", pa.string()),
        pa.field("high_watermark_json", pa.string()),
    ]
)


@dataclass(frozen=True)
class ResultPartReceipt:
    path: str
    rows: int
    sha256: str


class BoundedParquetResultWriter:
    """Write bounded batches with backpressure and atomic checkpoints."""

    _FLUSH = object()
    _STOP = object()

    def __init__(
        self,
        root: str | Path,
        *,
        run_id: str,
        profile_hash: str,
        strategy_hash: str,
        source_pin: str,
        mode: WriterMode = "research",
        batch_size: int = 10_000,
        queue_size: int = 20_000,
        resume: bool = False,
    ) -> None:
        self.root = Path(root).resolve()
        self.run_id = str(run_id)
        self.profile_hash = str(profile_hash)
        self.strategy_hash = str(strategy_hash)
        self.source_pin = str(source_pin)
        self.mode = mode
        self.batch_size = max(1, int(batch_size))
        self.queue_size = max(1, int(queue_size))
        if mode not in {"research", "audit"}:
            raise ValueError(f"unsupported writer mode: {mode}")
        if self.root.exists() and not resume:
            raise FileExistsError(f"result writer target exists: {self.root}")
        self.root.mkdir(parents=True, exist_ok=resume)
        self._parts: list[ResultPartReceipt] = []
        self._rows_written = 0
        self._part_number = 0
        self._last_high_watermark: Mapping[str, Any] | None = None
        self._backpressure_count = 0
        self._error: BaseException | None = None
        self._closed = False
        if resume:
            self._restore_checkpoint()
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=self.queue_size)
        self._thread = threading.Thread(
            target=self._run, name=f"fill-only-writer-{self.run_id}", daemon=True
        )
        self._thread.start()

    @property
    def rows_written(self) -> int:
        return self._rows_written

    @property
    def backpressure_count(self) -> int:
        return self._backpressure_count

    def submit(
        self,
        result: Any,
        *,
        profile: str,
        high_watermark: Mapping[str, Any] | None = None,
    ) -> None:
        payload = result.as_dict() if hasattr(result, "as_dict") else dict(result)
        fills = list(payload.get("fills") or [])
        evidence = payload.get("evidence_tier")
        row = {
            "run_id": self.run_id,
            "profile": str(profile),
            "order_id": str(payload.get("order_id") or ""),
            "status": str(payload.get("status") or ""),
            "filled_size": str(payload.get("filled_size") or "0"),
            "avg_price": str(payload.get("avg_price") or "0"),
            "reason": str(
                payload.get("reason") or payload.get("reason_unfilled") or ""
            ),
            "evidence_tier": str(evidence) if evidence is not None else None,
            "result_role": str(payload.get("result_role"))
            if payload.get("result_role") is not None
            else None,
            "is_modeled": _is_modeled(payload),
            "result_json": _canonical_json(payload) if self.mode == "audit" else None,
            "source_fills_json": _canonical_json(fills)
            if self.mode == "audit"
            else None,
            "high_watermark_json": _canonical_json(dict(high_watermark))
            if high_watermark is not None
            else None,
        }
        self.submit_row(row, high_watermark=high_watermark)

    def submit_row(
        self,
        row: Mapping[str, Any],
        *,
        high_watermark: Mapping[str, Any] | None = None,
    ) -> None:
        self._raise_if_failed()
        if self._closed:
            raise RuntimeError("result writer is closed")
        if self._queue.full():
            self._backpressure_count += 1
        self._queue.put((dict(row), dict(high_watermark) if high_watermark else None))

    def flush(self) -> None:
        self._raise_if_failed()
        event = threading.Event()
        self._queue.put((self._FLUSH, event))
        event.wait()
        self._queue.join()
        self._raise_if_failed()

    def close(self, *, finalize: bool = True) -> dict[str, Any] | None:
        if self._closed:
            return self._load_manifest() if finalize else None
        event = threading.Event()
        self._queue.put((self._STOP, event))
        event.wait()
        self._thread.join()
        self._queue.join()
        self._closed = True
        self._raise_if_failed()
        if not finalize:
            return None
        payload = self._manifest_payload()
        _atomic_json(self.root / "manifest.json", payload)
        return payload

    def _run(self) -> None:
        batch: list[dict[str, Any]] = []
        try:
            while True:
                payload, metadata = self._queue.get()
                try:
                    if payload is self._FLUSH:
                        self._write_batch(batch)
                        batch = []
                        metadata.set()
                        continue
                    if payload is self._STOP:
                        self._write_batch(batch)
                        batch = []
                        metadata.set()
                        return
                    row = payload
                    batch.append(row)
                    if metadata is not None:
                        self._last_high_watermark = metadata
                    if len(batch) >= self.batch_size:
                        self._write_batch(batch)
                        batch = []
                finally:
                    self._queue.task_done()
        except Exception as exc:  # noqa: BLE001  # pragma: no cover
            self._error = exc
            if "metadata" in locals() and isinstance(metadata, threading.Event):
                metadata.set()

    def _write_batch(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=RESULT_SCHEMA)
        relative = Path(f"part-{self._part_number:08d}.parquet")
        target = self.root / relative
        temporary = target.with_suffix(".parquet.tmp")
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            row_group_size=len(rows),
            write_statistics=True,
        )
        os.replace(temporary, target)
        receipt = ResultPartReceipt(
            path=relative.as_posix(), rows=len(rows), sha256=_file_sha256(target)
        )
        self._parts.append(receipt)
        self._rows_written += len(rows)
        self._part_number += 1
        _atomic_json(self.root / "checkpoint.json", self._checkpoint_payload())

    def _checkpoint_payload(self) -> dict[str, Any]:
        draft = {
            "schema_version": WRITER_SCHEMA_VERSION,
            "run_id": self.run_id,
            "profile_hash": self.profile_hash,
            "strategy_hash": self.strategy_hash,
            "source_pin": self.source_pin,
            "mode": self.mode,
            "rows_written": self._rows_written,
            "next_part_number": self._part_number,
            "parts": [asdict(item) for item in self._parts],
            "high_watermark": self._last_high_watermark,
        }
        return {**draft, "checkpoint_sha256": _canonical_sha256(draft)}

    def _manifest_payload(self) -> dict[str, Any]:
        draft = {
            **{
                key: value
                for key, value in self._checkpoint_payload().items()
                if key != "checkpoint_sha256"
            },
            "backpressure_count": self._backpressure_count,
            "complete": True,
        }
        return {**draft, "manifest_sha256": _canonical_sha256(draft)}

    def _restore_checkpoint(self) -> None:
        checkpoint_path = self.root / "checkpoint.json"
        if not checkpoint_path.exists():
            return
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        digest = str(payload.pop("checkpoint_sha256", ""))
        if digest != _canonical_sha256(payload):
            raise ValueError("result writer checkpoint checksum mismatch")
        for key, expected in (
            ("run_id", self.run_id),
            ("profile_hash", self.profile_hash),
            ("strategy_hash", self.strategy_hash),
            ("source_pin", self.source_pin),
            ("mode", self.mode),
        ):
            if str(payload.get(key)) != str(expected):
                raise ValueError(f"result writer checkpoint {key} mismatch")
        self._parts = [ResultPartReceipt(**item) for item in payload.get("parts", [])]
        for item in self._parts:
            if _file_sha256(self.root / item.path) != item.sha256:
                raise ValueError(f"result writer part checksum mismatch: {item.path}")
        self._rows_written = int(payload.get("rows_written") or 0)
        self._part_number = int(payload.get("next_part_number") or 0)
        self._last_high_watermark = payload.get("high_watermark")

    def _load_manifest(self) -> dict[str, Any] | None:
        path = self.root / "manifest.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError("Parquet writer thread failed") from self._error

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc, traceback
        self.close(finalize=exc_type is None)


def _is_modeled(payload: Mapping[str, Any]) -> bool:
    evidence = str(payload.get("evidence_tier") or "")
    status = str(payload.get("status") or "")
    return evidence.startswith(("D_", "E_")) or status.startswith("MODELED_")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
