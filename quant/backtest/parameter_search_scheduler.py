"""Persistent scheduler for fill-first parameter search batches."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
import json
from typing import Any, Callable, Mapping, Sequence
import uuid

from quant.backtest.backtest_engine import create_and_execute_backtest
from quant.backtest.parameter_search_results import build_parameter_search_results_report
from quant.backtest.parameter_search_runner import normalize_parameter_search_execution_result


READY = "ready"
REVIEW = "review"
MISSING = "missing"
FAIL = "fail"
SCHEMA_VERSION = "fill_first_parameter_search_scheduler_v1"

Executor = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def create_parameter_search_batch(
    conn: Any,
    plan: Mapping[str, Any],
    *,
    source: str = "parameter-search-scheduler",
    strategy_name: str = "unknown",
    strategy_version: str = "unknown",
    universe_name: str | None = None,
    max_attempts: int = 2,
    created_by: str | None = None,
) -> int:
    """Persist a parameter search plan as claimable queue items."""

    normalized_plan = dict(plan or {})
    plan_items = [_as_mapping(item) for item in normalized_plan.get("plan_items") or []]
    attempts = max(1, int(max_attempts))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.parameter_search_batches (
                status, source, universe_name, strategy_name, strategy_version,
                plan, planned_run_count, queued_count, max_attempts, created_by
            )
            VALUES ('queued', %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING batch_id
            """,
            (
                str(source or "parameter-search-scheduler"),
                str(universe_name or normalized_plan.get("universe_name") or ""),
                str(strategy_name or "unknown"),
                str(strategy_version or "unknown"),
                _json_dumps(normalized_plan),
                len(plan_items),
                len(plan_items),
                attempts,
                created_by,
            ),
        )
        batch_id = int(cur.fetchone()["batch_id"])
        cur.executemany(
            """
            INSERT INTO quant.parameter_search_batch_items (
                batch_id, item_index, item_key, status, parameter_fingerprint,
                evidence_mode, parameters, request_payload, max_attempts
            )
            VALUES (%s, %s, %s, 'queued', %s, %s, %s::jsonb, %s::jsonb, %s)
            """,
            [
                (
                    batch_id,
                    index,
                    str(item.get("key") or f"item-{index}"),
                    str(item.get("parameter_fingerprint") or ""),
                    str(item.get("evidence_mode") or ""),
                    _json_dumps(_as_mapping(item.get("parameters"))),
                    _json_dumps(_payload_for_item(item)),
                    attempts,
                )
                for index, item in enumerate(plan_items, start=1)
            ],
        )
    refresh_parameter_search_batch_status(conn, batch_id=batch_id)
    return batch_id


def list_parameter_search_batches(conn: Any, *, status: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if status:
        filters.append("status = %s")
        params.append(str(status))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.parameter_search_batches
            {where_sql}
            ORDER BY updated_at DESC, batch_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_parameter_search_batch(conn: Any, *, batch_id: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.parameter_search_batches WHERE batch_id = %s", (int(batch_id),))
        row = cur.fetchone()
    return dict(row) if row else None


def get_parameter_search_batch_items(conn: Any, *, batch_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.parameter_search_batch_items
            WHERE batch_id = %s
            ORDER BY item_index ASC
            LIMIT %s
            """,
            (int(batch_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def claim_next_parameter_search_item(
    conn: Any,
    *,
    batch_id: int | None = None,
    worker_id: str | None = None,
    retry_failed: bool = True,
) -> dict[str, Any] | None:
    """Claim one queued or retryable failed item with SKIP LOCKED."""

    worker = worker_id or f"parameter-search-worker-{uuid.uuid4()}"
    batch_filter = "AND batch_id = %(batch_id)s" if batch_id is not None else ""
    retry_filter = "OR (status = 'failed' AND attempt_count < max_attempts)" if retry_failed else ""
    params = {"batch_id": int(batch_id) if batch_id is not None else None, "worker_id": worker}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH candidate AS (
                SELECT item_id
                FROM quant.parameter_search_batch_items
                WHERE (status = 'queued' {retry_filter})
                  {batch_filter}
                ORDER BY batch_id ASC, item_index ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE quant.parameter_search_batch_items i
            SET status = 'running',
                attempt_count = attempt_count + 1,
                worker_id = %(worker_id)s,
                error = NULL,
                claimed_at = now(),
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            FROM candidate
            WHERE i.item_id = candidate.item_id
            RETURNING i.*
            """,
            params,
        )
        row = cur.fetchone()
    if row:
        claimed = dict(row)
        refresh_parameter_search_batch_status(conn, batch_id=int(claimed["batch_id"]))
        return claimed
    return None


def mark_parameter_search_item_succeeded(
    conn: Any,
    *,
    item_id: int,
    result_row: Mapping[str, Any],
    run_id: int | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.parameter_search_batch_items
            SET status = 'succeeded',
                result_row = %s::jsonb,
                run_id = COALESCE(%s, run_id),
                error = NULL,
                finished_at = now(),
                updated_at = now()
            WHERE item_id = %s
            RETURNING *
            """,
            (_json_dumps(result_row), run_id, int(item_id)),
        )
        row = dict(cur.fetchone())
    refresh_parameter_search_batch_status(conn, batch_id=int(row["batch_id"]))
    return row


def mark_parameter_search_item_failed(conn: Any, *, item_id: int, error: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.parameter_search_batch_items
            SET status = 'failed',
                error = %s,
                finished_at = now(),
                updated_at = now()
            WHERE item_id = %s
            RETURNING *
            """,
            (str(error)[:4000], int(item_id)),
        )
        row = dict(cur.fetchone())
    refresh_parameter_search_batch_status(conn, batch_id=int(row["batch_id"]))
    return row


def cancel_parameter_search_batch(
    conn: Any,
    *,
    batch_id: int,
    reason: str | None = None,
    canceled_by: str | None = None,
) -> dict[str, Any]:
    """Cancel queued/running/retryable work without deleting completed evidence."""

    note = str(reason or "parameter search batch canceled").strip()
    actor = str(canceled_by or "").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.parameter_search_batch_items
            SET status = 'canceled',
                error = %s,
                worker_id = COALESCE(worker_id, %s),
                finished_at = now(),
                updated_at = now()
            WHERE batch_id = %s
              AND status IN ('queued', 'running', 'failed')
            RETURNING item_id
            """,
            (note, actor or None, int(batch_id)),
        )
        canceled_count = len(cur.fetchall())
        cur.execute(
            """
            UPDATE quant.parameter_search_batches
            SET status = 'canceled',
                error = %s,
                finished_at = now(),
                updated_at = now()
            WHERE batch_id = %s
            RETURNING *
            """,
            (note, int(batch_id)),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"parameter search batch not found: {batch_id}")
    report = refresh_parameter_search_batch_status(conn, batch_id=int(batch_id))
    return {"batch": dict(row), "canceled_item_count": canceled_count, "progress": report}


def requeue_parameter_search_items(
    conn: Any,
    *,
    batch_id: int,
    statuses: Sequence[str] = ("failed",),
    reset_attempts: bool = False,
    clear_results: bool = False,
) -> dict[str, Any]:
    """Move selected non-succeeded items back to queued for another worker pass."""

    allowed = {"failed", "canceled", "running", "queued"}
    selected = [str(status or "").strip().lower() for status in statuses if str(status or "").strip().lower() in allowed]
    if not selected:
        raise ValueError("at least one requeue status is required")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.parameter_search_batch_items
            SET status = 'queued',
                attempt_count = CASE WHEN %(reset_attempts)s THEN 0 ELSE attempt_count END,
                result_row = CASE WHEN %(clear_results)s THEN '{}'::jsonb ELSE result_row END,
                run_id = CASE WHEN %(clear_results)s THEN NULL ELSE run_id END,
                worker_id = NULL,
                error = NULL,
                claimed_at = NULL,
                finished_at = NULL,
                updated_at = now()
            WHERE batch_id = %(batch_id)s
              AND status = ANY(%(statuses)s)
              AND status <> 'succeeded'
            RETURNING item_id
            """,
            {
                "batch_id": int(batch_id),
                "statuses": selected,
                "reset_attempts": bool(reset_attempts),
                "clear_results": bool(clear_results),
            },
        )
        requeued_count = len(cur.fetchall())
        cur.execute(
            """
            UPDATE quant.parameter_search_batches
            SET status = 'queued',
                error = NULL,
                finished_at = NULL,
                updated_at = now()
            WHERE batch_id = %s
            RETURNING *
            """,
            (int(batch_id),),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"parameter search batch not found: {batch_id}")
    report = refresh_parameter_search_batch_status(conn, batch_id=int(batch_id))
    return {"batch": dict(row), "requeued_item_count": requeued_count, "progress": report}


def run_parameter_search_batch_worker(
    conn: Any,
    *,
    batch_id: int | None = None,
    max_items: int = 1,
    worker_id: str | None = None,
    retry_failed: bool = True,
    executor: Executor | None = None,
    stop_on_error: bool = False,
) -> dict[str, Any]:
    """Claim and execute up to max_items parameter search items."""

    worker = worker_id or f"parameter-search-worker-{uuid.uuid4()}"
    processed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for _ in range(max(1, int(max_items))):
        item = claim_next_parameter_search_item(conn, batch_id=batch_id, worker_id=worker, retry_failed=retry_failed)
        if not item:
            break
        try:
            result = _execute_item(conn, item, executor=executor)
            plan_item = _plan_item_from_row(item)
            result_row = normalize_parameter_search_execution_result(plan_item, result, index=int(item.get("item_index") or 1), conn=conn)
            run_id = _optional_int(result_row.get("run_id"))
            stored = mark_parameter_search_item_succeeded(conn, item_id=int(item["item_id"]), result_row=result_row, run_id=run_id)
            processed.append(stored)
        except Exception as exc:
            stored = mark_parameter_search_item_failed(conn, item_id=int(item["item_id"]), error=str(exc))
            processed.append(stored)
            errors.append(stored)
            if stop_on_error:
                break
    target_batch_id = batch_id or (int(processed[-1]["batch_id"]) if processed else None)
    progress = refresh_parameter_search_batch_status(conn, batch_id=target_batch_id) if target_batch_id else {}
    return {
        "schema_version": SCHEMA_VERSION,
        "status": FAIL if errors else READY,
        "worker_id": worker,
        "processed_count": len(processed),
        "error_count": len(errors),
        "processed_items": processed,
        "progress": progress,
    }


def refresh_parameter_search_batch_status(conn: Any, *, batch_id: int | None) -> dict[str, Any]:
    if batch_id is None:
        return {}
    batch = get_parameter_search_batch(conn, batch_id=int(batch_id))
    if not batch:
        return {}
    items = get_parameter_search_batch_items(conn, batch_id=int(batch_id))
    report = build_parameter_search_progress_report(batch, items)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.parameter_search_batches
            SET status = %(status)s,
                summary = %(summary)s::jsonb,
                planned_run_count = %(planned_run_count)s,
                queued_count = %(queued_count)s,
                running_count = %(running_count)s,
                succeeded_count = %(succeeded_count)s,
                failed_count = %(failed_count)s,
                retryable_count = %(retryable_count)s,
                error = %(error)s,
                started_at = CASE WHEN %(started)s THEN COALESCE(started_at, now()) ELSE started_at END,
                finished_at = CASE WHEN %(finished)s THEN COALESCE(finished_at, now()) ELSE NULL END,
                updated_at = now()
            WHERE batch_id = %(batch_id)s
            """,
            {
                "batch_id": int(batch_id),
                "status": report["status"],
                "summary": _json_dumps(report),
                "planned_run_count": report["planned_run_count"],
                "queued_count": report["queued_count"],
                "running_count": report["running_count"],
                "succeeded_count": report["succeeded_count"],
                "failed_count": report["failed_count"],
                "retryable_count": report["retryable_count"],
                "error": report.get("reason") if report["status"] in {FAIL, REVIEW} else None,
                "started": report["running_count"] > 0 or report["succeeded_count"] > 0 or report["failed_count"] > 0,
                "finished": report["terminal"],
            },
        )
    return report


def build_parameter_search_progress_report(batch: Mapping[str, Any], items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize queue progress and attach result coverage for succeeded items."""

    rows = [_as_mapping(item) for item in items]
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[str(row.get("status") or "unknown")] = status_counts.get(str(row.get("status") or "unknown"), 0) + 1
    queued_count = status_counts.get("queued", 0)
    running_count = status_counts.get("running", 0)
    succeeded_count = status_counts.get("succeeded", 0)
    failed_count = status_counts.get("failed", 0)
    canceled_count = status_counts.get("canceled", 0)
    retryable_count = sum(
        1
        for row in rows
        if str(row.get("status") or "") == "failed" and int(row.get("attempt_count") or 0) < int(row.get("max_attempts") or 1)
    )
    planned_count = len(rows) or int(batch.get("planned_run_count") or 0)
    terminal = bool(planned_count) and queued_count == 0 and running_count == 0 and retryable_count == 0
    result_rows = [_json_mapping(row.get("result_row")) for row in rows if str(row.get("status") or "") == "succeeded" and _json_mapping(row.get("result_row"))]
    plan = _json_mapping(batch.get("plan"))
    results_report = build_parameter_search_results_report(plan, result_rows)
    completion_pct = _pct(succeeded_count + failed_count - retryable_count, planned_count)
    if not planned_count:
        status = MISSING
        reason = "no parameter search items were scheduled"
    elif running_count:
        status = "running"
        reason = "parameter search items are currently running"
    elif queued_count or retryable_count:
        status = "queued"
        reason = "parameter search items remain queued or retryable"
    elif canceled_count:
        status = "canceled"
        reason = "parameter search batch was canceled before all items completed"
    elif failed_count:
        status = REVIEW
        reason = "some parameter search items failed permanently"
    elif results_report.get("status") == READY:
        status = READY
        reason = "parameter search batch completed and result coverage is ready"
    else:
        status = REVIEW
        reason = str(results_report.get("reason") or "parameter search results require review")
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch.get("batch_id"),
        "status": status,
        "reason": reason,
        "source": batch.get("source"),
        "universe_name": batch.get("universe_name"),
        "strategy_name": batch.get("strategy_name"),
        "strategy_version": batch.get("strategy_version"),
        "planned_run_count": planned_count,
        "queued_count": queued_count,
        "running_count": running_count,
        "succeeded_count": succeeded_count,
        "failed_count": failed_count,
        "canceled_count": canceled_count,
        "retryable_count": retryable_count,
        "terminal": terminal,
        "completion_pct": _decimal_text(completion_pct),
        "status_counts": status_counts,
        "parameter_search_results": results_report,
        "next_actions": _progress_next_actions(status, queued_count=queued_count, retryable_count=retryable_count, failed_count=failed_count, results_report=results_report),
    }


def parameter_search_progress_to_markdown(report: Mapping[str, Any]) -> str:
    results = _as_mapping(report.get("parameter_search_results"))
    lines = [
        f"# Fill-first Parameter Search Progress: {report.get('status')}",
        "",
        f"- batch_id: {report.get('batch_id') or '-'}",
        f"- universe: {report.get('universe_name') or '-'}",
        f"- planned: {report.get('planned_run_count', 0)}",
        f"- queued: {report.get('queued_count', 0)}",
        f"- running: {report.get('running_count', 0)}",
        f"- succeeded: {report.get('succeeded_count', 0)}",
        f"- failed: {report.get('failed_count', 0)}",
        f"- canceled: {report.get('canceled_count', 0)}",
        f"- retryable: {report.get('retryable_count', 0)}",
        f"- completion: {report.get('completion_pct', '0')}%",
        f"- result_status: {results.get('status') or '-'}",
        f"- result_coverage: {results.get('coverage_pct', '0')}%",
        f"- reason: {report.get('reason') or '-'}",
        "",
        "## Next Actions",
    ]
    actions = list(report.get("next_actions") or [])
    lines.extend(f"- {action}" for action in actions) if actions else lines.append("- none")
    return "\n".join(lines)


def _execute_item(conn: Any, item: Mapping[str, Any], *, executor: Executor | None) -> Mapping[str, Any]:
    payload = _json_mapping(item.get("request_payload"))
    if executor is not None:
        return _as_mapping(executor(payload))
    return _as_mapping(create_and_execute_backtest(conn, payload))


def _plan_item_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "key": row.get("item_key"),
        "parameter_index": row.get("item_index"),
        "evidence_mode": row.get("evidence_mode"),
        "parameter_fingerprint": row.get("parameter_fingerprint"),
        "parameters": _json_mapping(row.get("parameters")),
        "request_payload": _json_mapping(row.get("request_payload")),
    }


def _payload_for_item(item: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(_as_mapping(item.get("request_payload")))
    payload["parameter_fingerprint"] = item.get("parameter_fingerprint") or payload.get("parameter_fingerprint")
    payload["evidence_mode"] = item.get("evidence_mode") or payload.get("evidence_mode")
    return payload


def _progress_next_actions(
    status: str,
    *,
    queued_count: int,
    retryable_count: int,
    failed_count: int,
    results_report: Mapping[str, Any],
) -> list[str]:
    if queued_count or retryable_count:
        return ["Run parameter search workers until queued and retryable items reach zero."]
    if failed_count:
        return ["Inspect failed item errors, fix the data or payload issue, then requeue or recreate the batch."]
    if results_report.get("status") != READY:
        return [str(action) for action in results_report.get("next_actions", [])[:5]]
    return ["Review parameter_search_results and production staging before approving parameters."]


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, Mapping) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_plain(value: Any) -> Any:
    if is_dataclass(value):
        return _as_plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _as_plain(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_as_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_as_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(_as_plain(value), ensure_ascii=False, sort_keys=True, default=str)


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except Exception:
        return None


def _pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator) * Decimal("100")


def _decimal_text(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.0001")).normalize(), "f")
    return "0" if text == "-0" else text
