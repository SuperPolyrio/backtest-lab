"""Process-isolated execution profiles sharing immutable Parquet inputs."""

from __future__ import annotations

import ctypes
import gc
import hashlib
import json
import multiprocessing
import os
import resource
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

import pyarrow as pa

from quant.backtest.disk_liquidity_ledger import DiskLiquidityLedger
from quant.backtest.frozen_order_catalog import FrozenOrderCatalog
from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger,
    V2TakerOrder,
    get_v2_execution_profile,
    replay_v2_taker_orders_with_diagnostics,
    with_v2_execution_profile,
)
from quant.backtest.parquet_result_writer import BoundedParquetResultWriter
from quant.backtest.trade_catalog import ParquetTradeCatalog
from quant.backtest.trade_only_v3 import (
    RunLiquidityLedger,
    TradeOnlyOrder,
    get_trade_only_profile,
    replay_trade_only_orders_with_diagnostics,
    with_trade_only_profile,
)


@dataclass(frozen=True)
class ProfileWorkerTask:
    execution_family: Literal["V2", "V3"]
    profile: str
    trade_catalog_path: str
    order_catalog_path: str
    result_path: str
    run_id: str
    matcher_backend: Literal["auto", "python", "rust"] = "auto"
    batch_size: int = 10_000
    writer_mode: Literal["research", "audit"] = "research"
    resume: bool = False
    max_batches: int | None = None
    max_orders: int | None = None
    skip_orders: int = 0
    chunk_mode: Literal["rows", "signal_day"] = "rows"
    chunk_days: int = 1
    recycle_batches: int | None = None
    inputs_preverified: bool = False


@dataclass(frozen=True)
class ProfileWorkerReceipt:
    schema_version: str
    execution_family: str
    profile: str
    run_id: str
    worker_pid: int
    orders: int
    statuses: dict[str, int]
    results_sha256: str
    ledger_sha256: str
    elapsed_seconds: float
    candidate_rows_scanned: int
    naive_rows_scanned: int
    clickhouse_query_count: int
    peak_rss_kb: int
    result_path: str
    batches: int
    complete: bool
    batch_timings_path: str = ""
    batch_timings_sha256: str = ""
    arrow_memory_pool: str = ""
    ledger_entries: dict[str, int] | None = None
    matching_backends: dict[str, int] | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_profile_workers(
    tasks: list[ProfileWorkerTask], *, max_workers: int
) -> list[ProfileWorkerReceipt]:
    if not tasks:
        return []
    verified_trades: dict[str, ParquetTradeCatalog] = {}
    verified_orders: set[tuple[str, str]] = set()
    prepared_tasks: list[ProfileWorkerTask] = []
    for task in tasks:
        trade_catalog = verified_trades.get(task.trade_catalog_path)
        if trade_catalog is None:
            trade_catalog = ParquetTradeCatalog(task.trade_catalog_path)
            verified_trades[task.trade_catalog_path] = trade_catalog
        order_key = (task.order_catalog_path, trade_catalog.source_pin)
        if order_key not in verified_orders:
            FrozenOrderCatalog(
                task.order_catalog_path,
                source_pin=trade_catalog.source_pin,
            )
            verified_orders.add(order_key)
        prepared_tasks.append(replace(task, inputs_preverified=True))
    tasks = prepared_tasks
    workers = max(1, min(int(max_workers), len(tasks)))
    groups = _partition_profile_tasks(tasks, workers=workers)
    if any(task.recycle_batches for task in tasks):
        if any(task.max_batches is not None for task in tasks):
            raise ValueError("recycle_batches cannot be combined with max_batches")
        if any(len(group) > 1 for group in groups):
            return _run_recycled_profile_groups(
                groups,
                tasks=tasks,
                max_workers=max(1, min(workers, len(groups))),
            )
        return _run_recycled_profile_workers(tasks, max_workers=workers)
    if len(groups) == 1:
        receipts = _run_profile_worker_group(groups[0])
        return _receipts_in_task_order(tasks, receipts)
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=max(1, min(workers, len(groups))), mp_context=context
    ) as pool:
        nested = list(pool.map(_run_profile_worker_group, groups))
    receipts = [receipt for group_receipts in nested for receipt in group_receipts]
    return _receipts_in_task_order(tasks, receipts)


def _partition_profile_tasks(
    tasks: list[ProfileWorkerTask], *, workers: int
) -> list[list[ProfileWorkerTask]]:
    if len(tasks) <= 1:
        return [list(tasks)]
    first_key = _shared_worker_key(tasks[0])
    if any(_shared_worker_key(task) != first_key for task in tasks[1:]):
        return [[task] for task in tasks]
    group_count = max(1, min(int(workers), len(tasks)))
    return [list(tasks[index::group_count]) for index in range(group_count)]


def _shared_worker_key(task: ProfileWorkerTask) -> tuple[Any, ...]:
    return (
        task.execution_family,
        task.trade_catalog_path,
        task.order_catalog_path,
        task.run_id,
        task.matcher_backend,
        task.batch_size,
        task.writer_mode,
        task.resume,
        task.max_batches,
        task.max_orders,
        task.skip_orders,
        task.chunk_mode,
        task.chunk_days,
        task.recycle_batches,
        task.inputs_preverified,
        _checkpoint_cursor_hint(task),
    )


def _checkpoint_cursor_hint(task: ProfileWorkerTask) -> tuple[int, int, bool] | None:
    if not task.resume:
        return None
    path = Path(task.result_path) / "runner_checkpoint.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return (
        int(payload.get("orders") or 0),
        int(payload.get("batches") or 0),
        bool(payload.get("complete")),
    )


def _receipts_in_task_order(
    tasks: list[ProfileWorkerTask], receipts: list[ProfileWorkerReceipt]
) -> list[ProfileWorkerReceipt]:
    by_path = {receipt.result_path: receipt for receipt in receipts}
    return [by_path[str(Path(task.result_path).resolve())] for task in tasks]


def _run_recycled_profile_workers(
    tasks: list[ProfileWorkerTask], *, max_workers: int
) -> list[ProfileWorkerReceipt]:
    pending = list(tasks)
    totals: dict[str, dict[str, Any]] = {
        task.result_path: {"elapsed_seconds": 0.0, "peak_rss_kb": 0, "segments": []}
        for task in tasks
    }
    completed: dict[str, ProfileWorkerReceipt] = {}
    previous_orders: dict[str, int] = {}
    while pending:
        wave = [
            replace(
                task,
                max_batches=max(1, int(task.recycle_batches or 1_000_000_000)),
            )
            for task in pending
        ]
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=max(1, min(max_workers, len(wave))),
            mp_context=context,
            max_tasks_per_child=1,
        ) as pool:
            receipts = list(pool.map(_run_profile_worker, wave))
        next_pending: list[ProfileWorkerTask] = []
        for task, receipt in zip(pending, receipts, strict=True):
            total = totals[task.result_path]
            total["elapsed_seconds"] += receipt.elapsed_seconds
            total["peak_rss_kb"] = max(total["peak_rss_kb"], receipt.peak_rss_kb)
            total["segments"].append(
                {
                    "worker_pid": receipt.worker_pid,
                    "orders": receipt.orders,
                    "batches": receipt.batches,
                    "elapsed_seconds": receipt.elapsed_seconds,
                    "peak_rss_kb": receipt.peak_rss_kb,
                    "complete": receipt.complete,
                }
            )
            if receipt.complete:
                aggregate = replace(
                    receipt,
                    elapsed_seconds=float(total["elapsed_seconds"]),
                    peak_rss_kb=int(total["peak_rss_kb"]),
                )
                completed[task.result_path] = aggregate
                _atomic_json(
                    Path(task.result_path) / "recycle_receipt.json",
                    {
                        "schema_version": "UnifiedFillOnlyWorkerRecycleReceiptV1",
                        "profile": task.profile,
                        "recycle_batches": task.recycle_batches,
                        "segments": total["segments"],
                        "aggregate": aggregate.as_dict(),
                    },
                )
                continue
            if previous_orders.get(task.result_path) == receipt.orders:
                raise RuntimeError("recycled profile worker made no progress")
            previous_orders[task.result_path] = receipt.orders
            next_pending.append(replace(task, resume=True))
        pending = next_pending
    return [completed[task.result_path] for task in tasks]


def _run_recycled_profile_groups(
    groups: list[list[ProfileWorkerTask]],
    *,
    tasks: list[ProfileWorkerTask],
    max_workers: int,
) -> list[ProfileWorkerReceipt]:
    pending = [list(group) for group in groups]
    totals: dict[str, dict[str, Any]] = {
        task.result_path: {"elapsed_seconds": 0.0, "peak_rss_kb": 0, "segments": []}
        for task in tasks
    }
    completed: dict[str, ProfileWorkerReceipt] = {}
    previous_orders: dict[str, int] = {}
    while pending:
        wave = [
            [
                replace(
                    task,
                    max_batches=max(
                        1, int(task.recycle_batches or 1_000_000_000)
                    ),
                )
                for task in group
            ]
            for group in pending
        ]
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=max(1, min(max_workers, len(wave))),
            mp_context=context,
            max_tasks_per_child=1,
        ) as pool:
            receipt_groups = list(pool.map(_run_profile_worker_group, wave))
        next_pending: list[list[ProfileWorkerTask]] = []
        for group, receipts in zip(pending, receipt_groups, strict=True):
            by_path = {receipt.result_path: receipt for receipt in receipts}
            next_group: list[ProfileWorkerTask] = []
            for task in group:
                receipt = by_path[str(Path(task.result_path).resolve())]
                total = totals[task.result_path]
                total["elapsed_seconds"] += receipt.elapsed_seconds
                total["peak_rss_kb"] = max(
                    int(total["peak_rss_kb"]), receipt.peak_rss_kb
                )
                total["segments"].append(
                    {
                        "worker_pid": receipt.worker_pid,
                        "orders": receipt.orders,
                        "batches": receipt.batches,
                        "elapsed_seconds": receipt.elapsed_seconds,
                        "peak_rss_kb": receipt.peak_rss_kb,
                        "complete": receipt.complete,
                        "shared_tape_profile_count": len(group),
                    }
                )
                if receipt.complete:
                    aggregate = replace(
                        receipt,
                        elapsed_seconds=float(total["elapsed_seconds"]),
                        peak_rss_kb=int(total["peak_rss_kb"]),
                    )
                    completed[task.result_path] = aggregate
                    _atomic_json(
                        Path(task.result_path) / "recycle_receipt.json",
                        {
                            "schema_version": "UnifiedFillOnlyWorkerRecycleReceiptV1",
                            "profile": task.profile,
                            "recycle_batches": task.recycle_batches,
                            "shared_tape_profile_count": len(group),
                            "segments": total["segments"],
                            "aggregate": aggregate.as_dict(),
                        },
                    )
                    continue
                if previous_orders.get(task.result_path) == receipt.orders:
                    raise RuntimeError("recycled profile worker made no progress")
                previous_orders[task.result_path] = receipt.orders
                next_group.append(replace(task, resume=True))
            if next_group:
                next_pending.append(next_group)
        pending = next_pending
    return [completed[task.result_path] for task in tasks]


@dataclass
class _ProfileRuntime:
    task: ProfileWorkerTask
    profile_hash: str
    result_path: Path
    checkpoint_path: Path
    writer: BoundedParquetResultWriter
    v2_ledger: CapacityLedger
    v3_ledger: RunLiquidityLedger
    resolved_v2_profile: Any | None
    resolved_v3_profile: Any | None
    statuses: Counter[str]
    order_count: int
    candidate_rows: int
    naive_rows: int
    batches: int
    batch_hashes: list[str]
    delta_receipts: list[dict[str, Any]]
    active_ledger_receipt: dict[str, Any] | None
    batch_timings_path: Path
    batch_timings: list[dict[str, Any]]
    matching_backends: Counter[str]
    disk_ledger: DiskLiquidityLedger
    started: float


def _run_profile_worker_group(
    tasks: list[ProfileWorkerTask],
) -> list[ProfileWorkerReceipt]:
    if not tasks:
        return []
    if len(tasks) == 1:
        return [_run_profile_worker(tasks[0])]
    completed_tasks = [
        task
        for task in tasks
        if (hint := _checkpoint_cursor_hint(task)) is not None and hint[2]
    ]
    pending_tasks = [task for task in tasks if task not in completed_tasks]
    completed = [_run_profile_worker(task) for task in completed_tasks]
    if not pending_tasks:
        return _receipts_in_task_order(tasks, completed)
    if len(pending_tasks) == 1:
        completed.append(_run_profile_worker(pending_tasks[0]))
        return _receipts_in_task_order(tasks, completed)

    arrow_memory_pool = _configure_arrow_memory_pool()
    first = pending_tasks[0]
    trade_catalog = ParquetTradeCatalog(
        first.trade_catalog_path,
        verify_checksums=not first.inputs_preverified,
    )
    order_catalog = FrozenOrderCatalog(
        first.order_catalog_path,
        source_pin=trade_catalog.source_pin,
        verify_checksums=not first.inputs_preverified,
    )
    if order_catalog.execution_family != first.execution_family:
        raise ValueError("profile task execution family does not match order catalog")
    runtimes = [
        _open_profile_runtime(
            task,
            trade_catalog=trade_catalog,
            order_catalog=order_catalog,
        )
        for task in pending_tasks
    ]
    cursors = {(state.order_count, state.batches) for state in runtimes}
    if len(cursors) != 1:
        _close_profile_runtimes(runtimes, finalize=False)
        raise ValueError("shared-tape profile checkpoints are not at the same cursor")

    batches_this_call = 0
    stopped_early = False
    start_offset = max(0, int(first.skip_orders)) + runtimes[0].order_count
    try:
        order_batches = (
            order_catalog.iter_signal_day_batches(
                read_batch_size=first.batch_size,
                days_per_batch=max(1, int(first.chunk_days)),
                start_offset=start_offset,
            )
            if first.chunk_mode == "signal_day"
            else order_catalog.iter_batches(
                batch_size=first.batch_size,
                start_offset=start_offset,
            )
        )
        for raw_batch in order_batches:
            current_order_count = runtimes[0].order_count
            if first.max_orders is not None and current_order_count >= int(
                first.max_orders
            ):
                break
            if first.max_orders is not None:
                remaining_limit = int(first.max_orders) - current_order_count
                raw_batch = raw_batch[: max(0, remaining_limit)]
                if not raw_batch:
                    break

            batch_started = perf_counter()
            catalog_order_start = max(0, int(first.skip_orders)) + current_order_count
            signal_days = sorted(
                {
                    signal_ts.date().isoformat()
                    for order in raw_batch
                    if (signal_ts := getattr(order, "signal_ts", None)) is not None
                }
            )
            adapt_started = perf_counter()
            orders_by_path: dict[str, list[Any]] = {}
            rows: list[Any]
            union_orders: list[Any]
            if first.execution_family == "V2":
                for state in runtimes:
                    assert state.resolved_v2_profile is not None
                    rows = [
                        with_v2_execution_profile(order, state.resolved_v2_profile)
                        for order in raw_batch
                        if isinstance(order, V2TakerOrder)
                    ]
                    if len(rows) != len(raw_batch):
                        raise TypeError("V2 worker received a non-V2 order")
                    orders_by_path[state.task.result_path] = rows
                union_orders = [
                    order for rows in orders_by_path.values() for order in rows
                ]
            else:
                for state in runtimes:
                    assert state.resolved_v3_profile is not None
                    rows = [
                        with_trade_only_profile(order, state.resolved_v3_profile)
                        for order in raw_batch
                        if isinstance(order, TradeOnlyOrder)
                    ]
                    if len(rows) != len(raw_batch):
                        raise TypeError("V3 worker received a non-V3 order")
                    orders_by_path[state.task.result_path] = rows
                union_orders = [
                    order for rows in orders_by_path.values() for order in rows
                ]
            adaptation_seconds = perf_counter() - adapt_started
            prepare_started = perf_counter()
            prepared = _prepare_trade_tape(
                trade_catalog,
                union_orders,
                matcher_backend=first.matcher_backend,
            )
            prepare_seconds = perf_counter() - prepare_started
            view_prepare_started = perf_counter()
            prepared_by_path = {
                state.task.result_path: trade_catalog.prepared_view_for_orders(
                    prepared, orders_by_path[state.task.result_path]
                )
                for state in runtimes
            }
            view_prepare_seconds = perf_counter() - view_prepare_started
            timing_rows: list[tuple[_ProfileRuntime, dict[str, Any]]] = []

            for state in runtimes:
                orders = orders_by_path[state.task.result_path]
                profile_prepared = prepared_by_path[state.task.result_path]
                pruning = _prune_active_ledger(
                    state.task,
                    batches=state.batches,
                    prepared=profile_prepared,
                    v2_ledger=state.v2_ledger,
                    v3_ledger=state.v3_ledger,
                    orders=orders,
                )
                match_started = perf_counter()
                results: list[Any]
                diagnostics: Any
                if first.execution_family == "V2":
                    state.v2_ledger.begin_delta_tracking()
                    results, _, diagnostics = replay_v2_taker_orders_with_diagnostics(
                        orders,
                        ledger=state.v2_ledger,
                        prepared_tape=profile_prepared,
                        backend=state.task.matcher_backend,
                    )
                    ledger_delta = state.v2_ledger.drain_delta().as_dict()
                else:
                    state.v3_ledger.begin_delta_tracking()
                    results, _, diagnostics = replay_trade_only_orders_with_diagnostics(
                        orders,
                        None,
                        state.task.profile,
                        ledger=state.v3_ledger,
                        prepared_tape=profile_prepared,
                        backend=state.task.matcher_backend,
                    )
                    ledger_delta = state.v3_ledger.drain_delta()
                matching_seconds = perf_counter() - match_started
                state.candidate_rows += diagnostics.candidate_rows_scanned
                state.naive_rows += diagnostics.naive_rows_scanned
                state.matching_backends[diagnostics.matching_backend] += 1

                persist_started = perf_counter()
                batch_digest = hashlib.sha256()
                for result in results:
                    payload = result.as_dict()
                    batch_digest.update(_canonical_json(payload).encode())
                    batch_digest.update(b"\n")
                    state.statuses[result.status] += 1
                    state.order_count += 1
                    state.writer.submit(
                        payload,
                        profile=state.task.profile,
                        high_watermark={"orders_processed": state.order_count},
                    )
                state.writer.flush()
                persist_seconds = perf_counter() - persist_started
                state.batches += 1
                state.batch_hashes.append(batch_digest.hexdigest())
                delta_receipt = _write_ledger_delta(
                    state.result_path, state.batches - 1, ledger_delta
                )
                state.delta_receipts.append(delta_receipt)
                state.disk_ledger.apply_delta(
                    state.batches - 1,
                    delta_receipt["sha256"],
                    ledger_delta,
                )
                state.active_ledger_receipt = _write_active_ledger_snapshot(
                    state.result_path,
                    state.v2_ledger.snapshot()
                    if first.execution_family == "V2"
                    else state.v3_ledger.snapshot(),
                )
                checkpoint_started = perf_counter()
                _write_runner_checkpoint(
                    state.checkpoint_path,
                    task=state.task,
                    profile_hash=state.profile_hash,
                    source_pin=trade_catalog.source_pin,
                    strategy_hash=order_catalog.strategy_hash,
                    orders=state.order_count,
                    statuses=state.statuses,
                    candidate_rows=state.candidate_rows,
                    naive_rows=state.naive_rows,
                    batches=state.batches,
                    batch_hashes=state.batch_hashes,
                    delta_receipts=state.delta_receipts,
                    active_ledger_receipt=state.active_ledger_receipt,
                    complete=False,
                )
                checkpoint_seconds = perf_counter() - checkpoint_started
                timing_rows.append(
                    (
                        state,
                        {
                            "schema_version": "UnifiedFillOnlyBatchTimingV1",
                            "batch": state.batches,
                            "catalog_order_start": catalog_order_start,
                            "catalog_order_end": catalog_order_start + len(raw_batch),
                            "orders": len(raw_batch),
                            "signal_day_start": signal_days[0] if signal_days else None,
                            "signal_day_end": signal_days[-1] if signal_days else None,
                            "trade_rows_indexed": profile_prepared.trade_rows_indexed,
                            "shared_union_trade_rows": prepared.trade_rows_indexed,
                            "adaptation_seconds": adaptation_seconds,
                            "prepare_seconds": prepare_seconds,
                            "amortized_prepare_seconds": prepare_seconds
                            / len(runtimes),
                            "profile_view_prepare_seconds": view_prepare_seconds
                            / len(runtimes),
                            "shared_tape_profile_count": len(runtimes),
                            "matching_seconds": matching_seconds,
                            "matching_backend": diagnostics.matching_backend,
                            "backend_fallback_reason": diagnostics.backend_fallback_reason,
                            "persist_seconds": persist_seconds,
                            "checkpoint_seconds": checkpoint_seconds,
                            "arrow_memory_pool": arrow_memory_pool,
                            "capacity_pruning": pruning,
                        },
                    )
                )
                del results

            batch_total_seconds = perf_counter() - batch_started
            del (
                prepared,
                prepared_by_path,
                profile_prepared,
                union_orders,
                orders,
                orders_by_path,
                raw_batch,
            )
            _release_batch_memory()
            for state, timing in timing_rows:
                timing.update(
                    {
                        "total_seconds": batch_total_seconds,
                        "peak_rss_kb": int(
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                        ),
                        "retained_rss_kb": _current_rss_kb(),
                    }
                )
                state.batch_timings.append(timing)
                _atomic_json(state.batch_timings_path, state.batch_timings)
            batches_this_call += 1
            if first.max_batches is not None and batches_this_call >= max(
                1, int(first.max_batches)
            ):
                stopped_early = not (
                    first.max_orders is not None
                    and runtimes[0].order_count >= int(first.max_orders)
                )
                break
    except Exception:
        _close_profile_runtimes(runtimes, finalize=False)
        raise

    receipts = [
        _finalize_profile_runtime(
            state,
            trade_catalog=trade_catalog,
            order_catalog=order_catalog,
            arrow_memory_pool=arrow_memory_pool,
            stopped_early=stopped_early,
        )
        for state in runtimes
    ]
    return _receipts_in_task_order(tasks, [*completed, *receipts])


def _open_profile_runtime(
    task: ProfileWorkerTask,
    *,
    trade_catalog: ParquetTradeCatalog,
    order_catalog: FrozenOrderCatalog,
) -> _ProfileRuntime:
    started = perf_counter()
    profile_hash = _profile_hash(task.execution_family, task.profile)
    result_path = Path(task.result_path).resolve()
    checkpoint_path = result_path / "runner_checkpoint.json"
    checkpoint = (
        _load_runner_checkpoint(
            checkpoint_path,
            task=task,
            profile_hash=profile_hash,
            source_pin=trade_catalog.source_pin,
            strategy_hash=order_catalog.strategy_hash,
        )
        if task.resume
        else None
    )
    writer = BoundedParquetResultWriter(
        result_path,
        run_id=task.run_id,
        profile_hash=profile_hash,
        strategy_hash=order_catalog.strategy_hash,
        source_pin=trade_catalog.source_pin,
        mode=task.writer_mode,
        batch_size=max(1, int(task.batch_size)),
        resume=bool(checkpoint),
    )
    v2_ledger = CapacityLedger()
    v3_ledger = RunLiquidityLedger(f"{task.run_id}:{task.profile}")
    batch_timings_path = result_path / "batch_timings.json"
    batch_timings = (
        json.loads(batch_timings_path.read_text(encoding="utf-8"))
        if checkpoint and batch_timings_path.exists()
        else []
    )
    active_ledger_receipt = (checkpoint or {}).get("active_ledger")
    if active_ledger_receipt:
        active_path = result_path / active_ledger_receipt["path"]
        if _file_sha256(active_path) != active_ledger_receipt["sha256"]:
            raise ValueError("active liquidity ledger snapshot checksum mismatch")
        active_payload = json.loads(active_path.read_text(encoding="utf-8"))
        if task.execution_family == "V2":
            v2_ledger = CapacityLedger.from_snapshot(active_payload)
        else:
            v3_ledger = RunLiquidityLedger.from_snapshot(active_payload)
    disk_ledger = DiskLiquidityLedger(
        result_path / "cumulative_ledger.sqlite3",
        execution_family=task.execution_family,
        ledger_id=f"{task.run_id}:{task.profile}",
        source_pin=trade_catalog.source_pin,
        profile_hash=profile_hash,
        strategy_hash=order_catalog.strategy_hash,
    )
    delta_receipts = list((checkpoint or {}).get("ledger_deltas") or [])
    for batch_number, receipt in enumerate(delta_receipts):
        delta_path = result_path / receipt["path"]
        needs_disk_delta = not disk_ledger.has_delta(batch_number, receipt["sha256"])
        if needs_disk_delta or not active_ledger_receipt:
            if _file_sha256(delta_path) != receipt["sha256"]:
                raise ValueError(f"ledger delta checksum mismatch: {receipt['path']}")
            delta = json.loads(delta_path.read_text(encoding="utf-8"))
            if needs_disk_delta:
                disk_ledger.apply_delta(batch_number, receipt["sha256"], delta)
            if not active_ledger_receipt:
                if task.execution_family == "V2":
                    v2_ledger.apply_delta(delta)
                else:
                    v3_ledger.apply_delta(delta)
    return _ProfileRuntime(
        task=task,
        profile_hash=profile_hash,
        result_path=result_path,
        checkpoint_path=checkpoint_path,
        writer=writer,
        v2_ledger=v2_ledger,
        v3_ledger=v3_ledger,
        resolved_v2_profile=(
            get_v2_execution_profile(task.profile)
            if task.execution_family == "V2"
            else None
        ),
        resolved_v3_profile=(
            get_trade_only_profile(task.profile)
            if task.execution_family == "V3"
            else None
        ),
        statuses=Counter((checkpoint or {}).get("statuses") or {}),
        order_count=int((checkpoint or {}).get("orders") or 0),
        candidate_rows=int((checkpoint or {}).get("candidate_rows_scanned") or 0),
        naive_rows=int((checkpoint or {}).get("naive_rows_scanned") or 0),
        batches=int((checkpoint or {}).get("batches") or 0),
        batch_hashes=list((checkpoint or {}).get("batch_result_hashes") or []),
        delta_receipts=delta_receipts,
        active_ledger_receipt=active_ledger_receipt,
        batch_timings_path=batch_timings_path,
        batch_timings=batch_timings,
        matching_backends=Counter(
            str(row.get("matching_backend") or "unknown") for row in batch_timings
        ),
        disk_ledger=disk_ledger,
        started=started,
    )


def _finalize_profile_runtime(
    state: _ProfileRuntime,
    *,
    trade_catalog: ParquetTradeCatalog,
    order_catalog: FrozenOrderCatalog,
    arrow_memory_pool: str,
    stopped_early: bool,
) -> ProfileWorkerReceipt:
    state.writer.close(finalize=not stopped_early)
    ledger_sha256 = (
        "" if stopped_early else state.disk_ledger.canonical_sha256()
    )
    ledger_entries = None if stopped_early else state.disk_ledger.entry_counts()
    receipt = ProfileWorkerReceipt(
        schema_version="UnifiedFillOnlyProfileWorkerReceiptV1",
        execution_family=state.task.execution_family,
        profile=state.task.profile,
        run_id=state.task.run_id,
        worker_pid=os.getpid(),
        orders=state.order_count,
        statuses=dict(sorted(state.statuses.items())),
        results_sha256=hashlib.sha256("".join(state.batch_hashes).encode()).hexdigest(),
        ledger_sha256=ledger_sha256,
        elapsed_seconds=perf_counter() - state.started,
        candidate_rows_scanned=state.candidate_rows,
        naive_rows_scanned=state.naive_rows,
        clickhouse_query_count=trade_catalog.clickhouse_query_count,
        peak_rss_kb=int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        result_path=str(state.result_path),
        batches=state.batches,
        complete=not stopped_early,
        batch_timings_path=str(state.batch_timings_path),
        batch_timings_sha256=(
            _file_sha256(state.batch_timings_path)
            if state.batch_timings_path.exists()
            else ""
        ),
        arrow_memory_pool=arrow_memory_pool,
        ledger_entries=ledger_entries,
        matching_backends=dict(sorted(state.matching_backends.items())),
    )
    _write_runner_checkpoint(
        state.checkpoint_path,
        task=state.task,
        profile_hash=state.profile_hash,
        source_pin=trade_catalog.source_pin,
        strategy_hash=order_catalog.strategy_hash,
        orders=state.order_count,
        statuses=state.statuses,
        candidate_rows=state.candidate_rows,
        naive_rows=state.naive_rows,
        batches=state.batches,
        batch_hashes=state.batch_hashes,
        delta_receipts=state.delta_receipts,
        active_ledger_receipt=state.active_ledger_receipt,
        complete=not stopped_early,
        receipt=receipt,
    )
    state.disk_ledger.close()
    return receipt


def _close_profile_runtimes(
    runtimes: list[_ProfileRuntime], *, finalize: bool
) -> None:
    for state in runtimes:
        try:
            state.writer.close(finalize=finalize)
        finally:
            state.disk_ledger.close()


def _run_profile_worker(task: ProfileWorkerTask) -> ProfileWorkerReceipt:
    started = perf_counter()
    arrow_memory_pool = _configure_arrow_memory_pool()
    trade_catalog = ParquetTradeCatalog(
        task.trade_catalog_path,
        verify_checksums=not task.inputs_preverified,
    )
    order_catalog = FrozenOrderCatalog(
        task.order_catalog_path,
        source_pin=trade_catalog.source_pin,
        verify_checksums=not task.inputs_preverified,
    )
    if order_catalog.execution_family != task.execution_family:
        raise ValueError("profile task execution family does not match order catalog")
    profile_hash = _profile_hash(task.execution_family, task.profile)
    result_path = Path(task.result_path).resolve()
    checkpoint_path = result_path / "runner_checkpoint.json"
    checkpoint = (
        _load_runner_checkpoint(
            checkpoint_path,
            task=task,
            profile_hash=profile_hash,
            source_pin=trade_catalog.source_pin,
            strategy_hash=order_catalog.strategy_hash,
        )
        if task.resume
        else None
    )
    if checkpoint and checkpoint.get("complete") and checkpoint.get("receipt"):
        return ProfileWorkerReceipt(**checkpoint["receipt"])
    writer = BoundedParquetResultWriter(
        result_path,
        run_id=task.run_id,
        profile_hash=profile_hash,
        strategy_hash=order_catalog.strategy_hash,
        source_pin=trade_catalog.source_pin,
        mode=task.writer_mode,
        batch_size=max(1, int(task.batch_size)),
        resume=bool(checkpoint),
    )
    v2_ledger = CapacityLedger()
    v3_ledger = RunLiquidityLedger(f"{task.run_id}:{task.profile}")
    resolved_v2_profile = (
        get_v2_execution_profile(task.profile)
        if task.execution_family == "V2"
        else None
    )
    resolved_v3_profile = (
        get_trade_only_profile(task.profile)
        if task.execution_family == "V3"
        else None
    )
    statuses: Counter[str] = Counter((checkpoint or {}).get("statuses") or {})
    order_count = int((checkpoint or {}).get("orders") or 0)
    candidate_rows = int((checkpoint or {}).get("candidate_rows_scanned") or 0)
    naive_rows = int((checkpoint or {}).get("naive_rows_scanned") or 0)
    batches = int((checkpoint or {}).get("batches") or 0)
    batch_hashes = list((checkpoint or {}).get("batch_result_hashes") or [])
    delta_receipts = list((checkpoint or {}).get("ledger_deltas") or [])
    active_ledger_receipt = (checkpoint or {}).get("active_ledger")
    batch_timings_path = result_path / "batch_timings.json"
    batch_timings = (
        json.loads(batch_timings_path.read_text(encoding="utf-8"))
        if checkpoint and batch_timings_path.exists()
        else []
    )
    matching_backends: Counter[str] = Counter(
        str(row.get("matching_backend") or "unknown") for row in batch_timings
    )
    if active_ledger_receipt:
        active_path = result_path / active_ledger_receipt["path"]
        if _file_sha256(active_path) != active_ledger_receipt["sha256"]:
            raise ValueError("active liquidity ledger snapshot checksum mismatch")
        active_payload = json.loads(active_path.read_text(encoding="utf-8"))
        if task.execution_family == "V2":
            v2_ledger = CapacityLedger.from_snapshot(active_payload)
        else:
            v3_ledger = RunLiquidityLedger.from_snapshot(active_payload)
    disk_ledger = DiskLiquidityLedger(
        result_path / "cumulative_ledger.sqlite3",
        execution_family=task.execution_family,
        ledger_id=f"{task.run_id}:{task.profile}",
        source_pin=trade_catalog.source_pin,
        profile_hash=profile_hash,
        strategy_hash=order_catalog.strategy_hash,
    )
    for batch_number, receipt in enumerate(delta_receipts):
        delta_path = result_path / receipt["path"]
        needs_disk_delta = not disk_ledger.has_delta(
            batch_number, receipt["sha256"]
        )
        if needs_disk_delta or not active_ledger_receipt:
            if _file_sha256(delta_path) != receipt["sha256"]:
                raise ValueError(f"ledger delta checksum mismatch: {receipt['path']}")
            delta = json.loads(delta_path.read_text(encoding="utf-8"))
            if needs_disk_delta:
                disk_ledger.apply_delta(batch_number, receipt["sha256"], delta)
            if not active_ledger_receipt:
                if task.execution_family == "V2":
                    v2_ledger.apply_delta(delta)
                else:
                    v3_ledger.apply_delta(delta)
    batches_this_call = 0
    stopped_early = False
    start_offset = max(0, int(task.skip_orders)) + order_count
    try:
        order_batches = (
            order_catalog.iter_signal_day_batches(
                read_batch_size=task.batch_size,
                days_per_batch=max(1, int(task.chunk_days)),
                start_offset=start_offset,
            )
            if task.chunk_mode == "signal_day"
            else order_catalog.iter_batches(
                batch_size=task.batch_size,
                start_offset=start_offset,
            )
        )
        for raw_batch in order_batches:
            if task.max_orders is not None and order_count >= int(task.max_orders):
                break
            if task.max_orders is not None:
                remaining_limit = int(task.max_orders) - order_count
                raw_batch = raw_batch[: max(0, remaining_limit)]
                if not raw_batch:
                    break
            batch_started = perf_counter()
            catalog_order_start = max(0, int(task.skip_orders)) + order_count
            signal_days = sorted(
                {
                    signal_ts.date().isoformat()
                    for order in raw_batch
                    if (signal_ts := getattr(order, "signal_ts", None)) is not None
                }
            )
            adapt_started = perf_counter()
            results: list[Any]
            if task.execution_family == "V2":
                assert resolved_v2_profile is not None
                v2_orders = [
                    with_v2_execution_profile(order, resolved_v2_profile)
                    for order in raw_batch
                    if isinstance(order, V2TakerOrder)
                ]
                if len(v2_orders) != len(raw_batch):
                    raise TypeError("V2 worker received a non-V2 order")
                adaptation_seconds = perf_counter() - adapt_started
                prepare_started = perf_counter()
                prepared = _prepare_trade_tape(
                    trade_catalog,
                    v2_orders,
                    matcher_backend=task.matcher_backend,
                )
                prepare_seconds = perf_counter() - prepare_started
                pruning = _prune_active_ledger(
                    task,
                    batches=batches,
                    prepared=prepared,
                    v2_ledger=v2_ledger,
                    v3_ledger=v3_ledger,
                    orders=v2_orders,
                )
                v2_ledger.begin_delta_tracking()
                match_started = perf_counter()
                v2_results, _, v2_diagnostics = replay_v2_taker_orders_with_diagnostics(
                    v2_orders,
                    ledger=v2_ledger,
                    prepared_tape=prepared,
                    backend=task.matcher_backend,
                )
                matching_seconds = perf_counter() - match_started
                ledger_delta = v2_ledger.drain_delta().as_dict()
                candidate_rows += v2_diagnostics.candidate_rows_scanned
                naive_rows += v2_diagnostics.naive_rows_scanned
                matching_backend = v2_diagnostics.matching_backend
                backend_fallback_reason = v2_diagnostics.backend_fallback_reason
                results = list(v2_results)
            else:
                assert resolved_v3_profile is not None
                v3_orders = [
                    with_trade_only_profile(order, resolved_v3_profile)
                    for order in raw_batch
                    if isinstance(order, TradeOnlyOrder)
                ]
                if len(v3_orders) != len(raw_batch):
                    raise TypeError("V3 worker received a non-V3 order")
                adaptation_seconds = perf_counter() - adapt_started
                prepare_started = perf_counter()
                prepared = _prepare_trade_tape(
                    trade_catalog,
                    v3_orders,
                    matcher_backend=task.matcher_backend,
                )
                prepare_seconds = perf_counter() - prepare_started
                pruning = _prune_active_ledger(
                    task,
                    batches=batches,
                    prepared=prepared,
                    v2_ledger=v2_ledger,
                    v3_ledger=v3_ledger,
                    orders=v3_orders,
                )
                v3_ledger.begin_delta_tracking()
                match_started = perf_counter()
                v3_results, _, v3_diagnostics = (
                    replay_trade_only_orders_with_diagnostics(
                        v3_orders,
                        None,
                        task.profile,
                        ledger=v3_ledger,
                        prepared_tape=prepared,
                        backend=task.matcher_backend,
                    )
                )
                matching_seconds = perf_counter() - match_started
                ledger_delta = v3_ledger.drain_delta()
                candidate_rows += v3_diagnostics.candidate_rows_scanned
                naive_rows += v3_diagnostics.naive_rows_scanned
                matching_backend = v3_diagnostics.matching_backend
                backend_fallback_reason = v3_diagnostics.backend_fallback_reason
                results = list(v3_results)
            matching_backends[matching_backend] += 1
            persist_started = perf_counter()
            batch_digest = hashlib.sha256()
            for result in results:
                payload = result.as_dict()
                batch_digest.update(_canonical_json(payload).encode())
                batch_digest.update(b"\n")
                statuses[result.status] += 1
                order_count += 1
                writer.submit(
                    payload,
                    profile=task.profile,
                    high_watermark={"orders_processed": order_count},
                )
            writer.flush()
            persist_seconds = perf_counter() - persist_started
            batches += 1
            batches_this_call += 1
            batch_hashes.append(batch_digest.hexdigest())
            delta_receipt = _write_ledger_delta(
                result_path, batches - 1, ledger_delta
            )
            delta_receipts.append(delta_receipt)
            disk_ledger.apply_delta(
                batches - 1,
                delta_receipt["sha256"],
                ledger_delta,
            )
            active_ledger_receipt = _write_active_ledger_snapshot(
                result_path,
                v2_ledger.snapshot()
                if task.execution_family == "V2"
                else v3_ledger.snapshot(),
            )
            checkpoint_started = perf_counter()
            _write_runner_checkpoint(
                checkpoint_path,
                task=task,
                profile_hash=profile_hash,
                source_pin=trade_catalog.source_pin,
                strategy_hash=order_catalog.strategy_hash,
                orders=order_count,
                statuses=statuses,
                candidate_rows=candidate_rows,
                naive_rows=naive_rows,
                batches=batches,
                batch_hashes=batch_hashes,
                delta_receipts=delta_receipts,
                active_ledger_receipt=active_ledger_receipt,
                complete=False,
            )
            checkpoint_seconds = perf_counter() - checkpoint_started
            batch_orders = len(raw_batch)
            trade_rows_indexed = prepared.trade_rows_indexed
            del results, raw_batch, prepared
            if task.execution_family == "V2":
                del v2_orders, v2_results
            else:
                del v3_orders, v3_results
            _release_batch_memory()
            batch_timings.append(
                {
                    "schema_version": "UnifiedFillOnlyBatchTimingV1",
                    "batch": batches,
                    "catalog_order_start": catalog_order_start,
                    "catalog_order_end": catalog_order_start + batch_orders,
                    "orders": batch_orders,
                    "signal_day_start": signal_days[0] if signal_days else None,
                    "signal_day_end": signal_days[-1] if signal_days else None,
                    "trade_rows_indexed": trade_rows_indexed,
                    "adaptation_seconds": adaptation_seconds,
                    "prepare_seconds": prepare_seconds,
                    "matching_seconds": matching_seconds,
                    "matching_backend": matching_backend,
                    "backend_fallback_reason": backend_fallback_reason,
                    "persist_seconds": persist_seconds,
                    "checkpoint_seconds": checkpoint_seconds,
                    "total_seconds": perf_counter() - batch_started,
                    "peak_rss_kb": int(
                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    ),
                    "retained_rss_kb": _current_rss_kb(),
                    "arrow_memory_pool": arrow_memory_pool,
                    "capacity_pruning": pruning,
                }
            )
            _atomic_json(batch_timings_path, batch_timings)
            if task.max_batches is not None and batches_this_call >= max(
                1, int(task.max_batches)
            ):
                stopped_early = not (
                    task.max_orders is not None
                    and order_count >= int(task.max_orders)
                )
                break
        writer.close(finalize=not stopped_early)
    except Exception:
        writer.close(finalize=False)
        disk_ledger.close()
        raise
    # Recycled workers only commit an incremental delta and cursor. Computing
    # the canonical cumulative ledger hash for every intermediate segment turns
    # an otherwise linear replay into O(number_of_segments * ledger_size).
    # The final worker computes the legacy-compatible hash exactly once.
    ledger_hash = "" if stopped_early else disk_ledger.canonical_sha256()
    ledger_entries = None if stopped_early else disk_ledger.entry_counts()
    result_hash = hashlib.sha256("".join(batch_hashes).encode()).hexdigest()
    batch_timings_hash = (
        _file_sha256(batch_timings_path) if batch_timings_path.exists() else ""
    )
    receipt = ProfileWorkerReceipt(
        schema_version="UnifiedFillOnlyProfileWorkerReceiptV1",
        execution_family=task.execution_family,
        profile=task.profile,
        run_id=task.run_id,
        worker_pid=os.getpid(),
        orders=order_count,
        statuses=dict(sorted(statuses.items())),
        results_sha256=result_hash,
        ledger_sha256=ledger_hash,
        elapsed_seconds=perf_counter() - started,
        candidate_rows_scanned=candidate_rows,
        naive_rows_scanned=naive_rows,
        clickhouse_query_count=trade_catalog.clickhouse_query_count,
        peak_rss_kb=int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        result_path=str(result_path),
        batches=batches,
        complete=not stopped_early,
        batch_timings_path=str(batch_timings_path),
        batch_timings_sha256=batch_timings_hash,
        arrow_memory_pool=arrow_memory_pool,
        ledger_entries=ledger_entries,
        matching_backends=dict(sorted(matching_backends.items())),
    )
    _write_runner_checkpoint(
        checkpoint_path,
        task=task,
        profile_hash=profile_hash,
        source_pin=trade_catalog.source_pin,
        strategy_hash=order_catalog.strategy_hash,
        orders=order_count,
        statuses=statuses,
        candidate_rows=candidate_rows,
        naive_rows=naive_rows,
        batches=batches,
        batch_hashes=batch_hashes,
        delta_receipts=delta_receipts,
        active_ledger_receipt=active_ledger_receipt,
        complete=not stopped_early,
        receipt=receipt,
    )
    disk_ledger.close()
    return receipt


def _profile_hash(family: str, profile: str) -> str:
    resolved = (
        get_v2_execution_profile(profile)
        if family == "V2"
        else get_trade_only_profile(profile)
    )
    payload = resolved.as_dict() if hasattr(resolved, "as_dict") else asdict(resolved)
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()


def _prepare_trade_tape(
    catalog: ParquetTradeCatalog,
    orders: list[Any],
    *,
    matcher_backend: str,
) -> Any:
    if str(matcher_backend).lower() in {"auto", "rust"}:
        return catalog.prepare_columnar_for_orders(orders)
    return catalog.prepare_for_orders(orders)


def _write_ledger_delta(
    root: Path, batch_number: int, payload: dict[str, Any]
) -> dict[str, Any]:
    directory = root / "ledger_deltas"
    directory.mkdir(parents=True, exist_ok=True)
    relative = Path("ledger_deltas") / f"delta-{batch_number:08d}.json"
    path = root / relative
    _atomic_json(path, payload)
    return {"path": relative.as_posix(), "sha256": _file_sha256(path)}


def _write_active_ledger_snapshot(
    root: Path, payload: dict[str, Any]
) -> dict[str, Any]:
    relative = Path("active_ledger.json")
    path = root / relative
    _atomic_json(path, payload)
    return {"path": relative.as_posix(), "sha256": _file_sha256(path)}


def _prune_active_ledger(
    task: ProfileWorkerTask,
    *,
    batches: int,
    prepared: Any,
    v2_ledger: CapacityLedger,
    v3_ledger: RunLiquidityLedger,
    orders: list[Any],
) -> dict[str, Any]:
    if task.chunk_mode != "signal_day" or batches <= 0:
        return {"enabled": False}
    source_trade_ids = (trade.trade_id for trade in prepared.ordered_trades)
    if task.execution_family == "V2":
        return {
            "enabled": True,
            "source_keys_dropped": v2_ledger.retain_source_trade_ids(
                source_trade_ids
            ),
        }
    arrivals = [
        int(order.arrival_ts.timestamp())
        for order in orders
        if getattr(order, "arrival_ts", None) is not None
    ]
    return {
        "enabled": True,
        **v3_ledger.prune(
            source_trade_ids=source_trade_ids,
            synthetic_not_before_epoch=min(arrivals) if arrivals else None,
        ),
    }


def _write_runner_checkpoint(
    path: Path,
    *,
    task: ProfileWorkerTask,
    profile_hash: str,
    source_pin: str,
    strategy_hash: str,
    orders: int,
    statuses: Counter[str],
    candidate_rows: int,
    naive_rows: int,
    batches: int,
    batch_hashes: list[str],
    delta_receipts: list[dict[str, Any]],
    active_ledger_receipt: dict[str, Any] | None,
    complete: bool,
    receipt: ProfileWorkerReceipt | None = None,
) -> None:
    draft = {
        "schema_version": "UnifiedFillOnlyProfileRunnerCheckpointV1",
        "execution_family": task.execution_family,
        "profile": task.profile,
        "run_id": task.run_id,
        "profile_hash": profile_hash,
        "source_pin": source_pin,
        "strategy_hash": strategy_hash,
        "batch_size": task.batch_size,
        "writer_mode": task.writer_mode,
        "matcher_backend": task.matcher_backend,
        "max_orders": task.max_orders,
        "skip_orders": task.skip_orders,
        "chunk_mode": task.chunk_mode,
        "chunk_days": task.chunk_days,
        "recycle_batches": task.recycle_batches,
        "orders": orders,
        "statuses": dict(sorted(statuses.items())),
        "candidate_rows_scanned": candidate_rows,
        "naive_rows_scanned": naive_rows,
        "batches": batches,
        "batch_result_hashes": batch_hashes,
        "ledger_deltas": delta_receipts,
        "active_ledger": active_ledger_receipt,
        "complete": complete,
        "receipt": receipt.as_dict() if receipt is not None else None,
    }
    _atomic_json(path, {**draft, "checkpoint_sha256": _sha256(draft)})


def _load_runner_checkpoint(
    path: Path,
    *,
    task: ProfileWorkerTask,
    profile_hash: str,
    source_pin: str,
    strategy_hash: str,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    digest = str(payload.pop("checkpoint_sha256", ""))
    if digest != _sha256(payload):
        raise ValueError("profile runner checkpoint checksum mismatch")
    for key, expected in (
        ("schema_version", "UnifiedFillOnlyProfileRunnerCheckpointV1"),
        ("execution_family", task.execution_family),
        ("profile", task.profile),
        ("run_id", task.run_id),
        ("profile_hash", profile_hash),
        ("source_pin", source_pin),
        ("strategy_hash", strategy_hash),
        ("batch_size", task.batch_size),
        ("writer_mode", task.writer_mode),
        ("matcher_backend", task.matcher_backend),
        ("max_orders", task.max_orders),
        ("skip_orders", task.skip_orders),
        ("chunk_mode", task.chunk_mode),
        ("chunk_days", task.chunk_days),
        ("recycle_batches", task.recycle_batches),
    ):
        actual = payload.get(key, 0) if key == "skip_orders" else payload.get(key)
        if actual != expected:
            raise ValueError(f"profile runner checkpoint {key} mismatch")
    return payload


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _release_batch_memory() -> None:
    gc.collect()
    pa.default_memory_pool().release_unused()
    try:
        malloc_trim = ctypes.CDLL(None).malloc_trim
    except (AttributeError, OSError):
        return
    malloc_trim.argtypes = [ctypes.c_size_t]
    malloc_trim.restype = ctypes.c_int
    malloc_trim(0)


def _current_rss_kb() -> int:
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def _configure_arrow_memory_pool() -> str:
    requested = os.getenv("FILL_ONLY_ARROW_MEMORY_POOL", "auto").strip().lower()
    supported = set(pa.supported_memory_backends())
    selected = "jemalloc" if requested == "auto" and "jemalloc" in supported else requested
    if selected in {"", "auto", "mimalloc"}:
        if selected == "mimalloc" and "mimalloc" in supported:
            pa.set_memory_pool(pa.mimalloc_memory_pool())
        return pa.default_memory_pool().backend_name
    if selected == "system":
        pa.set_memory_pool(pa.system_memory_pool())
        return "system"
    if selected == "jemalloc" and "jemalloc" in supported:
        pa.jemalloc_set_decay_ms(0)
        pa.set_memory_pool(pa.jemalloc_memory_pool())
        return "jemalloc"
    raise ValueError(f"unsupported Arrow memory pool: {requested!r}")
