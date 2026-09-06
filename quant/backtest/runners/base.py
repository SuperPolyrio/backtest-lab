"""Shared primitives for backtest validation runners."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import json
import resource
import time
from typing import Any, Iterator


RESULT_FIELDS = (
    "run_id",
    "data_version",
    "market_count",
    "rows_scanned",
    "bars_processed",
    "orders_created",
    "fills",
    "partial_fills",
    "rejected",
    "expired",
    "settlements",
    "db_query_sec",
    "engine_sec",
    "ledger_write_sec",
    "total_runtime_sec",
    "peak_memory_mb",
)


@dataclass
class ValidationResult:
    run_id: str
    data_version: str
    market_count: int = 0
    rows_scanned: int = 0
    bars_processed: int = 0
    orders_created: int = 0
    fills: int = 0
    partial_fills: int = 0
    rejected: int = 0
    expired: int = 0
    settlements: int = 0
    db_query_sec: float = 0.0
    engine_sec: float = 0.0
    ledger_write_sec: float = 0.0
    total_runtime_sec: float = 0.0
    peak_memory_mb: float = 0.0
    passed: bool = True
    message: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("db_query_sec", "engine_sec", "ledger_write_sec", "total_runtime_sec", "peak_memory_mb"):
            payload[key] = round(float(payload.get(key) or 0), 6)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, default=str)


class Timer:
    def __init__(self) -> None:
        self.elapsed: dict[str, float] = {}

    @contextmanager
    def track(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self.elapsed[name] = self.elapsed.get(name, 0.0) + (time.perf_counter() - start)


class MemorySampler:
    @staticmethod
    def peak_mb() -> float:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KiB, macOS reports bytes. This project runs on Linux, but
        # keep the fallback harmless for local dev machines.
        return float(usage) / (1024.0 if usage < 10_000_000_000 else 1024.0 * 1024.0)


def assert_result_passed(result: ValidationResult) -> None:
    if not result.passed:
        raise AssertionError(f"{result.run_id} failed: {result.message}")


def print_results(results: list[ValidationResult], *, json_output: bool = False) -> None:
    if json_output:
        print(json.dumps([item.as_dict() for item in results], sort_keys=True, default=str))
        return
    for result in results:
        print(result.to_json())
