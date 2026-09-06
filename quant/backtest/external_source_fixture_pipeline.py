"""End-to-end fixture pipeline for fill-first external evidence imports."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Mapping

from quant.backtest.configured_external_sources import (
    build_configured_external_source_report,
    load_external_source_env_files,
)
from quant.backtest.calibration import build_calibration_report, upsert_calibration_orders
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events
from quant.backtest.cost_calibration import normalize_real_cost_event, upsert_real_cost_events
from quant.backtest.cost_calibration import (
    build_cost_calibration_report,
    build_cost_calibration_samples,
    load_ledger_cost_rows_for_run,
    load_real_cost_events_for_run,
    upsert_cost_calibration_samples,
)
from quant.backtest.external_source_fixture import (
    RUN_PLAN_FIXTURE_SCHEMA_VERSION,
    build_fill_first_external_source_fixture,
    build_fill_first_external_source_fixture_from_plan,
)
from quant.backtest.external_signals import normalize_external_signal_event, upsert_external_signal_events
from quant.backtest.external_source_discovery import load_external_source_records
from quant.backtest.external_source_run_coverage import build_external_source_run_coverage_report
from quant.backtest.external_source_state import evaluate_external_source_import_health, load_external_source_import_states
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events
from quant.backtest.platform_incidents import normalize_platform_incident, upsert_platform_incidents
from quant.core.schema import create_schema


READY = "ready"
REVIEW = "review"
FAIL = "fail"
MISSING = "missing"
ROLLBACK_SMOKE_RUN_ID = -9001001
SCHEMA_ADVISORY_LOCK_KEY = 914020250607


def run_fill_first_external_source_fixture_pipeline(
    output_dir: Path,
    *,
    project_root: Path,
    run_id: int = 9001,
    source_prefix: str = "fixture",
    overwrite: bool = False,
    write: bool = False,
    check_db: bool = False,
    conn: Any | None = None,
    command_runner: Any | None = None,
    command_timeout_seconds: int = 300,
    rollback_smoke: bool = False,
    fixture_plan: Mapping[str, Any] | None = None,
    calibration_smoke: bool = False,
    build_calibration: bool = False,
) -> dict[str, Any]:
    """Generate fixture files, run configured imports, and optionally verify DB rows."""

    fixture = (
        build_fill_first_external_source_fixture_from_plan(
            output_dir,
            fixture_plan,
            source_prefix=source_prefix,
            overwrite=overwrite,
        )
        if fixture_plan is not None
        else build_fill_first_external_source_fixture(
            output_dir,
            run_id=run_id,
            source_prefix=source_prefix,
            overwrite=overwrite,
        )
    )
    env = load_external_source_env_files([fixture["files"]["env_file"]], base_env={})
    import_report = build_configured_external_source_report(
        env,
        project_root=project_root,
        dry_run=not write,
        command_timeout_seconds=command_timeout_seconds,
        command_runner=command_runner,
    )
    persisted_calibration = _persisted_calibration_report(
        conn,
        run_id=int(fixture.get("run_id") or run_id),
        source_prefix=source_prefix,
        enabled=bool(build_calibration and write and not rollback_smoke),
    )
    db_report = _rollback_smoke_report(
        conn,
        fixture=fixture,
        source_prefix=source_prefix,
        calibration_smoke=calibration_smoke,
    ) if rollback_smoke else _db_fixture_report(
        conn,
        run_id=int(fixture.get("run_id") or run_id),
        source_prefix=source_prefix,
        enabled=check_db,
        fixture=fixture,
    )
    run_coverage = _fixture_run_coverage_report(
        fixture,
        fixture_plan=fixture_plan,
        source_prefix=source_prefix,
    )
    status = _pipeline_status(fixture, import_report, db_report, run_coverage)
    return {
        "status": status,
        "write": bool(write),
        "check_db": bool(check_db),
        "rollback_smoke": bool(rollback_smoke),
        "run_id": fixture.get("run_id") or run_id,
        "source_prefix": source_prefix,
        "run_specific": fixture.get("schema_version") == RUN_PLAN_FIXTURE_SCHEMA_VERSION,
        "calibration_smoke": bool(calibration_smoke),
        "build_calibration": bool(build_calibration),
        "fixture": fixture,
        "imports": import_report,
        "calibration": persisted_calibration,
        "db": db_report,
        "run_coverage": run_coverage,
    }


def fill_first_external_source_fixture_pipeline_to_markdown(report: Mapping[str, Any]) -> str:
    fixture = report.get("fixture") if isinstance(report.get("fixture"), Mapping) else {}
    imports = report.get("imports") if isinstance(report.get("imports"), Mapping) else {}
    db = report.get("db") if isinstance(report.get("db"), Mapping) else {}
    counts = fixture.get("event_counts") if isinstance(fixture.get("event_counts"), Mapping) else {}
    lines = [
        f"# Fill-first External Source Fixture Pipeline: {report.get('status')}",
        "",
        f"- write: {report.get('write')}",
        f"- check_db: {report.get('check_db')}",
        f"- rollback_smoke: {report.get('rollback_smoke')}",
        f"- run_specific: {report.get('run_specific')}",
        f"- calibration_smoke: {report.get('calibration_smoke')}",
        f"- build_calibration: {report.get('build_calibration')}",
        f"- run_id: {report.get('run_id')}",
        f"- source_prefix: {report.get('source_prefix')}",
        f"- fixture: {fixture.get('status')}",
        f"- imports: {imports.get('status')} configured={imports.get('configured_count', 0)}",
        f"- db: {db.get('status')} ({db.get('reason')})",
        f"- order_state_events: {counts.get('order_state', 0)}",
        f"- cost_events: {counts.get('cost_events', 0)}",
        f"- platform_incidents: {counts.get('platform_incidents', 0)}",
        f"- external_signals: {counts.get('external_signals', 0)}",
        "",
        "| check | status | detail |",
        "| --- | --- | --- |",
        f"| fixture | {fixture.get('status')} | output `{fixture.get('output_dir')}` |",
        f"| imports | {imports.get('status')} | configured {imports.get('configured_count', 0)}, skipped {imports.get('skipped_count', 0)} |",
        f"| db | {db.get('status')} | {db.get('reason')} |",
    ]
    run_coverage = report.get("run_coverage") if isinstance(report.get("run_coverage"), Mapping) else {}
    if run_coverage:
        lines.append(
            f"| run coverage | {run_coverage.get('status')} | {run_coverage.get('reason')} |"
        )
    table_counts = db.get("table_counts") if isinstance(db.get("table_counts"), Mapping) else {}
    if table_counts:
        lines.extend(["", "## DB Counts", "", "| table | rows |", "| --- | ---: |"])
        for table, count in table_counts.items():
            lines.append(f"| `{table}` | {count} |")
    calibration = report.get("calibration") if isinstance(report.get("calibration"), Mapping) else {}
    db_calibration = db.get("calibration") if isinstance(db.get("calibration"), Mapping) else {}
    if not calibration or (calibration.get("reason") == "calibration_write_not_requested" and db_calibration):
        calibration = db_calibration
    if calibration:
        lines.extend(
            [
                "",
                "## Calibration",
                "",
                f"- status: {calibration.get('status')}",
                f"- reason: {calibration.get('reason')}",
                f"- samples_built: {calibration.get('samples_built', 0)}",
                f"- samples_written: {calibration.get('samples_written', 0)}",
                f"- cost_samples_built: {calibration.get('cost_samples_built', 0)}",
                f"- cost_samples_written: {calibration.get('cost_samples_written', 0)}",
                f"- trust: {calibration.get('report', {}).get('trust_status') if isinstance(calibration.get('report'), Mapping) else '-'}",
            ]
        )
    if run_coverage:
        lines.extend(
            [
                "",
                "## Run Coverage",
                "",
                f"- status: {run_coverage.get('status')}",
                f"- reason: {run_coverage.get('reason')}",
                f"- external_order_candidate_count: {run_coverage.get('external_order_candidate_count', 0)}",
                f"- order_state_coverage_pct: {run_coverage.get('order_state_coverage_pct', 0)}",
                f"- calibration_coverage_pct: {run_coverage.get('calibration_coverage_pct', 0)}",
                f"- real_cost_event_count: {run_coverage.get('real_cost_event_count', 0)}",
                f"- platform_incident_count: {run_coverage.get('platform_incident_count', 0)}",
            ]
        )
    return "\n".join(lines)


def _db_fixture_report(
    conn: Any | None,
    *,
    run_id: int,
    source_prefix: str,
    enabled: bool,
    fixture: Mapping[str, Any],
) -> dict[str, Any]:
    if not enabled:
        return {"status": REVIEW, "reason": "db_check_not_requested", "table_counts": {}, "health": {}}
    if conn is None:
        return {"status": REVIEW, "reason": "database_connection_not_provided", "table_counts": {}, "health": {}}
    table_counts = {
        "quant.real_order_state_events": _fetch_count(
            conn,
            "quant.real_order_state_events",
            "run_id = %s AND source = %s",
            (run_id, f"{source_prefix}-order-state"),
        ),
        "quant.real_backtest_cost_events": _fetch_count(
            conn,
            "quant.real_backtest_cost_events",
            "run_id = %s AND source = %s",
            (run_id, f"{source_prefix}-wallet"),
        ),
        "quant.platform_incidents": _fetch_count(
            conn,
            "quant.platform_incidents",
            "source = %s",
            (f"{source_prefix}-ops",),
        ),
        "quant.external_signal_events": _fetch_count(
            conn,
            "quant.external_signal_events",
            "run_id = %s AND source = %s",
            (run_id, f"{source_prefix}-signals"),
        ),
    }
    states = load_external_source_import_states(conn, limit=100)
    fixture_states = [
        state
        for state in states
        if str(state.get("state_key") or "").startswith(f"{source_prefix}-")
    ]
    health = evaluate_external_source_import_health(fixture_states, min_rows_written=1)
    expected = _expected_counts(fixture)
    required_ready = (
        table_counts["quant.real_order_state_events"] >= expected["order_state"]
        and table_counts["quant.real_backtest_cost_events"] >= expected["cost_events"]
        and table_counts["quant.platform_incidents"] >= expected["platform_incidents"]
        and table_counts["quant.external_signal_events"] >= expected["external_signals"]
        and health["status"] == READY
        and health["state_count"] >= 4
    )
    return {
        "status": READY if required_ready else REVIEW,
        "reason": "fixture_db_rows_ready" if required_ready else "fixture_db_rows_incomplete",
        "table_counts": table_counts,
        "health": health,
    }


def _rollback_smoke_report(
    conn: Any | None,
    *,
    fixture: Mapping[str, Any],
    source_prefix: str,
    calibration_smoke: bool = False,
) -> dict[str, Any]:
    if conn is None:
        return {"status": REVIEW, "reason": "database_connection_not_provided", "table_counts": {}, "health": {}}
    _acquire_schema_lock(conn)
    create_schema(conn)
    missing_tables = _missing_required_tables(conn, include_calibration=calibration_smoke)
    if missing_tables:
        return {
            "status": FAIL,
            "reason": "schema_missing",
            "missing_tables": missing_tables,
            "table_counts": {},
            "health": {},
            "rolled_back": False,
        }
    use_fixture_run = fixture.get("schema_version") == RUN_PLAN_FIXTURE_SCHEMA_VERSION and int(fixture.get("run_id") or 0) > 0
    run_id = int(fixture.get("run_id")) if use_fixture_run else _insert_rollback_smoke_run(conn, source_prefix=source_prefix)
    files = fixture.get("files") if isinstance(fixture.get("files"), Mapping) else {}
    order_rows = [
        normalize_order_state_event(row, source=f"{source_prefix}-order-state", run_id=run_id)
        for row in load_external_source_records(Path(str(files["order_state"])), "real_order_state_events")
    ]
    cost_rows = [
        normalize_real_cost_event(row, source=f"{source_prefix}-wallet", run_id=run_id)
        for row in load_external_source_records(Path(str(files["cost_events"])), "real_cost_events")
    ]
    incident_rows = [
        normalize_platform_incident(row, source=f"{source_prefix}-ops")
        for row in load_external_source_records(Path(str(files["platform_incidents"])), "platform_incidents")
    ]
    external_signal_rows = [
        normalize_external_signal_event(row, source=f"{source_prefix}-signals", run_id=run_id)
        for row in load_external_source_records(Path(str(files["external_signals"])), "external_signal_events")
    ]
    order_count = upsert_real_order_state_events(conn, order_rows)
    cost_count = upsert_real_cost_events(conn, cost_rows)
    incident_count = upsert_platform_incidents(conn, incident_rows)
    external_signal_count = upsert_external_signal_events(conn, external_signal_rows)
    calibration = _calibration_smoke_report(
        conn,
        run_id=run_id,
        order_rows=order_rows,
        source_prefix=source_prefix,
        enabled=calibration_smoke,
    )
    table_counts = {
        "quant.real_order_state_events": order_count,
        "quant.real_backtest_cost_events": cost_count,
        "quant.platform_incidents": incident_count,
        "quant.external_signal_events": external_signal_count,
    }
    expected = _expected_counts(fixture)
    calibration_ready = not calibration_smoke or calibration.get("status") == READY
    ready = (
        order_count >= expected["order_state"]
        and cost_count >= expected["cost_events"]
        and incident_count >= expected["platform_incidents"]
        and external_signal_count >= expected["external_signals"]
        and calibration_ready
    )
    with conn.cursor() as cur:
        cur.execute("ROLLBACK")
    return {
        "status": READY if ready else REVIEW,
        "reason": "rollback_smoke_ready" if ready else "rollback_smoke_incomplete",
        "table_counts": table_counts,
        "health": {"status": READY if ready else REVIEW, "reason": "transaction_rolled_back", "state_count": 0},
        "calibration": calibration,
        "rolled_back": True,
        "schema_initialized": True,
        "smoke_run_id": run_id,
    }


def _acquire_schema_lock(conn: Any) -> None:
    """Serialize schema initialization and rollback smoke writes in one DB transaction."""

    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_KEY,))


def _missing_required_tables(conn: Any, *, include_calibration: bool = False) -> list[str]:
    required = [
        "quant.quant_backtest_runs",
        "quant.quant_backtest_orders",
        "quant.real_order_state_events",
        "quant.real_backtest_cost_events",
        "quant.platform_incidents",
        "quant.external_signal_events",
    ]
    if include_calibration:
        required.append("quant.quant_backtest_calibration_orders")
    missing: list[str] = []
    for table in required:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) AS table_name", (table,))
            row = cur.fetchone()
        value = row.get("table_name") if isinstance(row, Mapping) else (row[0] if row else None)
        if value is None:
            missing.append(table)
    return missing


def _calibration_smoke_report(
    conn: Any,
    *,
    run_id: int,
    order_rows: list[Mapping[str, Any]],
    source_prefix: str,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {"status": REVIEW, "reason": "calibration_smoke_not_requested", "samples_built": 0, "samples_written": 0, "report": {}}
    orders = _fetch_orders_for_calibration(conn, run_id)
    samples = build_calibration_samples_from_order_events(
        orders,
        order_rows,
        source=f"{source_prefix}-calibration-smoke",
    )
    written = upsert_calibration_orders(conn, samples)
    report = build_calibration_report(samples)
    ready = len(orders) > 0 and len(samples) > 0 and written == len(samples)
    return {
        "status": READY if ready else REVIEW,
        "reason": "calibration_smoke_ready" if ready else "calibration_smoke_incomplete",
        "orders_read": len(orders),
        "events_read": len(order_rows),
        "samples_built": len(samples),
        "samples_written": written,
        "report": report,
    }


def _persisted_calibration_report(
    conn: Any | None,
    *,
    run_id: int,
    source_prefix: str,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {"status": REVIEW, "reason": "calibration_write_not_requested", "samples_built": 0, "samples_written": 0, "cost_samples_built": 0, "cost_samples_written": 0, "report": {}, "cost_report": {}}
    if conn is None:
        return {"status": REVIEW, "reason": "database_connection_not_provided", "samples_built": 0, "samples_written": 0, "cost_samples_built": 0, "cost_samples_written": 0, "report": {}, "cost_report": {}}
    orders = _fetch_orders_for_calibration(conn, run_id)
    order_events = _fetch_real_order_events_for_calibration(conn, run_id=run_id, source=f"{source_prefix}-order-state")
    samples = build_calibration_samples_from_order_events(
        orders,
        order_events,
        source=f"{source_prefix}-calibration",
    )
    samples_written = upsert_calibration_orders(conn, samples)
    report = build_calibration_report(samples)

    ledger_cost_rows = load_ledger_cost_rows_for_run(conn, run_id)
    real_cost_rows = load_real_cost_events_for_run(conn, run_id, source=f"{source_prefix}-wallet")
    cost_samples = build_cost_calibration_samples(
        ledger_cost_rows,
        real_cost_rows,
        source=f"{source_prefix}-cost-calibration",
        run_id=run_id,
    )
    cost_samples_written = upsert_cost_calibration_samples(conn, cost_samples) if cost_samples else 0
    cost_report = build_cost_calibration_report(cost_samples)
    ready = bool(samples and samples_written == len(samples))
    return {
        "status": READY if ready else REVIEW,
        "reason": "calibration_written" if ready else "calibration_incomplete",
        "orders_read": len(orders),
        "events_read": len(order_events),
        "samples_built": len(samples),
        "samples_written": samples_written,
        "cost_events_read": len(real_cost_rows),
        "cost_samples_built": len(cost_samples),
        "cost_samples_written": cost_samples_written,
        "report": report,
        "cost_report": cost_report,
    }


def _fixture_run_coverage_report(
    fixture: Mapping[str, Any],
    *,
    fixture_plan: Mapping[str, Any] | None,
    source_prefix: str,
) -> dict[str, Any]:
    """Build a run-level evidence coverage report directly from generated fixture files."""

    if fixture_plan is None:
        return {
            "status": REVIEW,
            "run_specific": False,
            "reason": "not_run_specific_fixture",
            "external_order_candidate_count": 0,
            "order_state_coverage_pct": "0",
            "calibration_coverage_pct": "0",
            "next_actions": ["Use --from-run-orders or --from-run-id to verify coverage for a concrete backtest run."],
        }
    run = dict(fixture_plan.get("run") or {})
    orders = [dict(row) for row in fixture_plan.get("orders") or [] if isinstance(row, Mapping)]
    files = fixture.get("files") if isinstance(fixture.get("files"), Mapping) else {}
    order_events = _load_fixture_records(files, "order_state", "real_order_state_events")
    cost_events = _load_fixture_records(files, "cost_events", "real_cost_events")
    incidents = _load_fixture_records(files, "platform_incidents", "platform_incidents")
    external_signals = _load_fixture_records(files, "external_signals", "external_signal_events")
    calibration_rows = build_calibration_samples_from_order_events(
        orders,
        order_events,
        source=f"{source_prefix}-coverage",
    )
    cost_calibration_rows = _fixture_cost_calibration_rows(cost_events)
    report = build_external_source_run_coverage_report(
        {
            "run": run,
            "orders": orders,
            "real_order_events": order_events,
            "calibration_rows": calibration_rows,
            "real_cost_events": cost_events,
            "cost_calibration_rows": cost_calibration_rows,
            "platform_incidents": incidents,
            "external_signal_events": external_signals,
            "external_states": _fixture_external_states(source_prefix),
        },
        run_id=int(run.get("run_id") or fixture.get("run_id") or 0) or None,
    )
    return {
        **report,
        "run_specific": True,
        "fixture_generated_calibration_sample_count": len(calibration_rows),
        "fixture_generated_cost_calibration_sample_count": len(cost_calibration_rows),
    }


def _load_fixture_records(files: Mapping[str, Any], key: str, source_type: str) -> list[dict[str, Any]]:
    path_text = str(files.get(key) or "").strip()
    if not path_text:
        return []
    return [dict(row) for row in load_external_source_records(Path(path_text), source_type)]


def _fixture_cost_calibration_rows(cost_events: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in cost_events:
        event_type = str(row.get("event_type") or "").upper()
        if event_type not in {"FEE", "REBATE"}:
            continue
        rows.append(
            {
                "run_id": row.get("run_id"),
                "order_id": row.get("order_id"),
                "trade_id": row.get("trade_id"),
                "event_type": event_type,
                "live_amount": row.get("amount"),
                "source": row.get("source"),
            }
        )
    return rows


def _fixture_external_states(source_prefix: str) -> list[dict[str, Any]]:
    return [
        {"state_key": f"{source_prefix}-order-state-events", "source_type": "real_order_state_events"},
        {"state_key": f"{source_prefix}-real-cost-events", "source_type": "real_cost_events"},
        {"state_key": f"{source_prefix}-platform-incidents", "source_type": "platform_incidents"},
        {"state_key": f"{source_prefix}-external-signals", "source_type": "external_signal_events"},
    ]


def _fetch_orders_for_calibration(conn: Any, run_id: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                o.*,
                r.market_slug AS run_market_slug,
                r.token_side AS run_token_side,
                r.price_source AS run_price_source,
                r.backtest_engine AS run_backtest_engine
            FROM quant.quant_backtest_orders o
            JOIN quant.quant_backtest_runs r ON r.run_id = o.run_id
            WHERE o.run_id = %s
            ORDER BY o.signal_index ASC, o.order_id ASC
            """,
            (run_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def _fetch_real_order_events_for_calibration(conn: Any, *, run_id: int, source: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.real_order_state_events
            WHERE run_id = %s
              AND source = %s
            ORDER BY COALESCE(event_time, created_at), event_id
            """,
            (run_id, source),
        )
        return [dict(row) for row in cur.fetchall()]


def _insert_rollback_smoke_run(conn: Any, *, source_prefix: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_runs (
                run_id, status, market_slug, token_side, price_source, backtest_engine,
                from_block, to_block, rows_processed, meta
            )
            VALUES (
                %s, 'succeeded', 'fixture-world-cup-winner', 'YES', 'orderfilled_block_close', 'builtin',
                88900000, 88900050, 0, %s::jsonb
            )
            RETURNING run_id
            """,
            (
                ROLLBACK_SMOKE_RUN_ID,
                json.dumps(
                    {
                        "fixture": True,
                        "source_prefix": source_prefix,
                        "purpose": "rollback external source fixture smoke",
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return int(row["run_id"])
    return int(row[0])


def _fetch_count(conn: Any, table: str, where_sql: str, params: tuple[Any, ...]) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE {where_sql}", params)
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return int(row.get("count") or 0)
    return int(row[0] or 0) if row else 0


def _expected_counts(fixture: Mapping[str, Any]) -> dict[str, int]:
    counts = fixture.get("event_counts") if isinstance(fixture.get("event_counts"), Mapping) else {}
    return {
        "order_state": int(counts.get("order_state") or 0),
        "cost_events": int(counts.get("cost_events") or 0),
        "platform_incidents": int(counts.get("platform_incidents") or 0),
        "external_signals": int(counts.get("external_signals") or 0),
    }


def _pipeline_status(
    fixture: Mapping[str, Any],
    import_report: Mapping[str, Any],
    db_report: Mapping[str, Any],
    run_coverage: Mapping[str, Any],
) -> str:
    if fixture.get("status") != READY or import_report.get("status") == FAIL:
        return FAIL
    if db_report.get("status") == FAIL:
        return FAIL
    if run_coverage.get("run_specific"):
        coverage_status = str(run_coverage.get("status") or "")
        if coverage_status in {FAIL, MISSING}:
            return FAIL
        if coverage_status == REVIEW:
            return REVIEW
    if db_report.get("status") == REVIEW and db_report.get("reason") not in {"db_check_not_requested"}:
        return REVIEW
    if import_report.get("status") != READY:
        return REVIEW
    return READY
