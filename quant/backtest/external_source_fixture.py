"""Generate deterministic local external evidence fixtures for fill-first checks."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
import json
from typing import Any, Mapping

from quant.backtest.configured_external_sources import build_configured_external_source_report
from quant.backtest.external_source_discovery import discover_external_source_files, preview_external_source_import
from quant.backtest.shadow_live_validation import validate_shadow_live_order_events


FIXTURE_SCHEMA_VERSION = "fill_first_external_source_fixture_v1"
RUN_PLAN_FIXTURE_SCHEMA_VERSION = "fill_first_external_source_run_plan_fixture_v1"


def build_fill_first_external_source_fixture(
    output_dir: Path,
    *,
    run_id: int = 9001,
    source_prefix: str = "fixture",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Write a small local evidence bundle and return validation reports."""

    target = Path(output_dir)
    if target.exists() and any(target.iterdir()) and not overwrite:
        raise FileExistsError(f"fixture output directory is not empty: {target}")
    target.mkdir(parents=True, exist_ok=True)

    order_state_path = target / "shadow_live_order_state_events.jsonl"
    cost_path = target / "real_cost_events.jsonl"
    incident_path = target / "platform_incidents.jsonl"
    external_signal_path = target / "external_signal_events.jsonl"
    env_path = target / "fill_first_external_sources.env"

    order_events = _order_state_events(run_id=run_id, source=f"{source_prefix}-order-state")
    cost_events = _cost_events(run_id=run_id, source=f"{source_prefix}-wallet")
    incidents = _platform_incidents(source=f"{source_prefix}-ops")
    external_signals = _external_signal_events(run_id=run_id, source=f"{source_prefix}-signals")

    _write_jsonl(order_state_path, order_events)
    _write_jsonl(cost_path, cost_events)
    _write_jsonl(incident_path, incidents)
    _write_jsonl(external_signal_path, external_signals)
    _write_env_file(
        env_path,
        {
            "ORDER_STATE_INPUT": str(order_state_path),
            "ORDER_STATE_SOURCE": f"{source_prefix}-order-state",
            "ORDER_STATE_RUN_ID": str(run_id),
            "ORDER_STATE_KEY": f"{source_prefix}-order-state-events",
            "COST_EVENTS_INPUT": str(cost_path),
            "COST_EVENTS_SOURCE": f"{source_prefix}-wallet",
            "COST_EVENTS_RUN_ID": str(run_id),
            "COST_EVENTS_STATE_KEY": f"{source_prefix}-real-cost-events",
            "PLATFORM_INCIDENTS_INPUT": str(incident_path),
            "PLATFORM_INCIDENTS_SOURCE": f"{source_prefix}-ops",
            "PLATFORM_INCIDENTS_STATE_KEY": f"{source_prefix}-platform-incidents",
            "EXTERNAL_SIGNAL_INPUT": str(external_signal_path),
            "EXTERNAL_SIGNAL_SOURCE": f"{source_prefix}-signals",
            "EXTERNAL_SIGNAL_RUN_ID": str(run_id),
            "EXTERNAL_SIGNAL_STATE_KEY": f"{source_prefix}-external-signals",
        },
    )

    validation = validate_shadow_live_order_events(order_events, require_cost_fields=True)
    discovery_candidates = discover_external_source_files([target])
    discovery_report = preview_external_source_import(discovery_candidates, base=target)
    configured_report = build_configured_external_source_report(
        _fixture_env(order_state_path, cost_path, incident_path, external_signal_path, run_id=run_id, source_prefix=source_prefix),
        project_root=Path(__file__).resolve().parents[2],
        dry_run=True,
    )
    return {
        "schema_version": FIXTURE_SCHEMA_VERSION,
        "status": _fixture_status(validation, discovery_report, configured_report),
        "output_dir": str(target),
        "run_id": run_id,
        "files": {
            "order_state": str(order_state_path),
            "cost_events": str(cost_path),
            "platform_incidents": str(incident_path),
            "external_signals": str(external_signal_path),
            "env_file": str(env_path),
        },
        "event_counts": {
            "order_state": len(order_events),
            "cost_events": len(cost_events),
            "platform_incidents": len(incidents),
            "external_signals": len(external_signals),
        },
        "validation": validation,
        "discovery": discovery_report,
        "configured_imports": configured_report,
    }


def build_fill_first_external_source_fixture_from_plan(
    output_dir: Path,
    plan: Mapping[str, Any],
    *,
    source_prefix: str = "run-fixture",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Write an external evidence bundle tied to a shadow/live order plan."""

    target = Path(output_dir)
    if target.exists() and any(target.iterdir()) and not overwrite:
        raise FileExistsError(f"fixture output directory is not empty: {target}")
    target.mkdir(parents=True, exist_ok=True)

    run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
    run_id = _int_or_none(run.get("run_id"))
    order_state_path = target / "shadow_live_order_state_events.jsonl"
    cost_path = target / "real_cost_events.jsonl"
    incident_path = target / "platform_incidents.jsonl"
    external_signal_path = target / "external_signal_events.jsonl"
    env_path = target / "fill_first_external_sources.env"

    order_events = _order_state_events_from_plan(plan, source=f"{source_prefix}-order-state")
    cost_events = _cost_events_from_plan(plan, source=f"{source_prefix}-wallet")
    incidents = _platform_incidents_from_plan(plan, source=f"{source_prefix}-ops")
    external_signals = _external_signal_events_from_plan(plan, source=f"{source_prefix}-signals")

    _write_jsonl(order_state_path, order_events)
    _write_jsonl(cost_path, cost_events)
    _write_jsonl(incident_path, incidents)
    _write_jsonl(external_signal_path, external_signals)
    _write_env_file(
        env_path,
        {
            "ORDER_STATE_INPUT": str(order_state_path),
            "ORDER_STATE_SOURCE": f"{source_prefix}-order-state",
            "ORDER_STATE_RUN_ID": str(run_id or ""),
            "ORDER_STATE_KEY": f"{source_prefix}-order-state-events",
            "COST_EVENTS_INPUT": str(cost_path),
            "COST_EVENTS_SOURCE": f"{source_prefix}-wallet",
            "COST_EVENTS_RUN_ID": str(run_id or ""),
            "COST_EVENTS_STATE_KEY": f"{source_prefix}-real-cost-events",
            "PLATFORM_INCIDENTS_INPUT": str(incident_path),
            "PLATFORM_INCIDENTS_SOURCE": f"{source_prefix}-ops",
            "PLATFORM_INCIDENTS_STATE_KEY": f"{source_prefix}-platform-incidents",
            "EXTERNAL_SIGNAL_INPUT": str(external_signal_path),
            "EXTERNAL_SIGNAL_SOURCE": f"{source_prefix}-signals",
            "EXTERNAL_SIGNAL_RUN_ID": str(run_id or ""),
            "EXTERNAL_SIGNAL_STATE_KEY": f"{source_prefix}-external-signals",
        },
    )

    validation = validate_shadow_live_order_events(order_events, require_cost_fields=True)
    discovery_candidates = discover_external_source_files([target])
    discovery_report = preview_external_source_import(discovery_candidates, base=target)
    configured_report = build_configured_external_source_report(
        _fixture_env(
            order_state_path,
            cost_path,
            incident_path,
            external_signal_path,
            run_id=run_id or 0,
            source_prefix=source_prefix,
        ),
        project_root=Path(__file__).resolve().parents[2],
        dry_run=True,
    )
    return {
        "schema_version": RUN_PLAN_FIXTURE_SCHEMA_VERSION,
        "status": _fixture_status(validation, discovery_report, configured_report),
        "output_dir": str(target),
        "run_id": run_id,
        "plan_status": plan.get("status"),
        "files": {
            "order_state": str(order_state_path),
            "cost_events": str(cost_path),
            "platform_incidents": str(incident_path),
            "external_signals": str(external_signal_path),
            "env_file": str(env_path),
        },
        "event_counts": {
            "order_state": len(order_events),
            "cost_events": len(cost_events),
            "platform_incidents": len(incidents),
            "external_signals": len(external_signals),
        },
        "validation": validation,
        "discovery": discovery_report,
        "configured_imports": configured_report,
    }


def fill_first_external_source_fixture_to_markdown(report: Mapping[str, Any]) -> str:
    files = report.get("files") if isinstance(report.get("files"), Mapping) else {}
    counts = report.get("event_counts") if isinstance(report.get("event_counts"), Mapping) else {}
    validation = report.get("validation") if isinstance(report.get("validation"), Mapping) else {}
    discovery = report.get("discovery") if isinstance(report.get("discovery"), Mapping) else {}
    configured = report.get("configured_imports") if isinstance(report.get("configured_imports"), Mapping) else {}
    lines = [
        f"# Fill-first External Source Fixture: {report.get('status')}",
        "",
        f"- schema: {report.get('schema_version')}",
        f"- output_dir: `{report.get('output_dir')}`",
        f"- run_id: {report.get('run_id')}",
        f"- order_state_events: {counts.get('order_state', 0)}",
        f"- cost_events: {counts.get('cost_events', 0)}",
        f"- platform_incidents: {counts.get('platform_incidents', 0)}",
        f"- external_signals: {counts.get('external_signals', 0)}",
        f"- validation: {validation.get('status')} ({validation.get('calibration_ready_count', 0)} calibration-ready)",
        f"- discovery: {discovery.get('status')} ({discovery.get('file_count', 0)} files)",
        f"- configured_imports: {configured.get('status')} ({configured.get('configured_count', 0)} configured)",
        "",
        "| file | path |",
        "| --- | --- |",
    ]
    for name, path in files.items():
        lines.append(f"| {name} | `{path}` |")
    lines.extend(
        [
            "",
            "## Dry-run import",
            "",
            "```bash",
            "conda run -n polyBacktest python scripts/run_configured_external_source_imports.py \\",
            f"  --env-file {files.get('env_file', '<env-file>')} \\",
            "  --format markdown",
            "```",
        ]
    )
    return "\n".join(lines)


def _fixture_status(
    validation: Mapping[str, Any],
    discovery_report: Mapping[str, Any],
    configured_report: Mapping[str, Any],
) -> str:
    if validation.get("status") != "ready":
        return "fail"
    if discovery_report.get("status") != "ready":
        return "fail"
    if configured_report.get("status") != "ready":
        return "fail"
    return "ready"


def _order_state_events(*, run_id: int, source: str) -> list[dict[str, Any]]:
    return [
        {
            "run_id": run_id,
            "order_id": "sim-ord-001",
            "external_order_id": "live-ord-001",
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-france",
            "token_side": "YES",
            "event_time": "2026-06-22T12:00:05Z",
            "event_type": "order_state",
            "source": source,
            "api_order_status": "FILLED",
            "clob_order_status": "FILLED",
            "submit_at": "2026-06-22T12:00:00Z",
            "accepted_at": "2026-06-22T12:00:01Z",
            "payload": {
                "live_status": "FILLED",
                "live_fill_price": "0.197",
                "live_fill_size": "25",
                "live_fee": "0.0125",
                "live_rebate": "0.0000",
                "live_cash_delta": "-4.9375",
                "live_position_delta": "25",
                "live_latency_seconds": "5",
                "maker_taker": "taker",
            },
        },
        {
            "run_id": run_id,
            "order_id": "sim-ord-002",
            "external_order_id": "live-ord-002",
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-spain",
            "token_side": "YES",
            "event_time": "2026-06-22T12:02:10Z",
            "event_type": "order_state",
            "source": source,
            "api_order_status": "PARTIAL_FILLED",
            "clob_order_status": "PARTIAL_FILLED",
            "submit_at": "2026-06-22T12:02:00Z",
            "accepted_at": "2026-06-22T12:02:01Z",
            "payload": {
                "live_status": "PARTIAL_FILLED",
                "live_fill_price": "0.141",
                "live_fill_size": "10",
                "live_fee": "0.0050",
                "live_rebate": "0.0000",
                "live_cash_delta": "-1.4150",
                "live_position_delta": "10",
                "live_latency_seconds": "10",
                "maker_taker": "maker",
            },
        },
        {
            "run_id": run_id,
            "order_id": "sim-ord-003",
            "external_order_id": "live-ord-003",
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-england",
            "token_side": "YES",
            "event_time": "2026-06-22T12:04:00Z",
            "event_type": "order_state",
            "source": source,
            "api_order_status": "REJECTED",
            "clob_order_status": "REJECTED",
            "submit_at": "2026-06-22T12:03:55Z",
            "payload": {
                "live_status": "REJECTED",
                "no_fill_reason": "post_only_would_take",
                "live_latency_seconds": "5",
            },
        },
    ]


def _cost_events(*, run_id: int, source: str) -> list[dict[str, Any]]:
    return [
        {
            "run_id": run_id,
            "cost_id": "fixture-fee-001",
            "source": source,
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-france",
            "token_side": "YES",
            "order_id": "sim-ord-001",
            "event_type": "FEE",
            "observed_at": "2026-06-22T12:00:05Z",
            "observed_block": 88900001,
            "amount": "0.0125",
            "currency": "USDC",
            "tx_hash": "0xfixturefee001",
        },
        {
            "run_id": run_id,
            "cost_id": "fixture-gas-001",
            "source": source,
            "market_slug": "fixture-world-cup-winner",
            "event_type": "GAS_COST",
            "observed_at": "2026-06-22T12:05:00Z",
            "observed_block": 88900020,
            "amount": "0.0200",
            "currency": "USDC",
            "tx_hash": "0xfixturegas001",
        },
    ]


def _platform_incidents(*, source: str) -> list[dict[str, Any]]:
    return [
        {
            "incident_key": "fixture-clob-latency-window",
            "source": source,
            "severity": "warning",
            "component": "clob",
            "title": "Fixture CLOB latency window",
            "description": "Synthetic incident used to verify fill-first environment flags.",
            "market_slug": "fixture-world-cup-winner",
            "start_ts": "2026-06-22T12:01:00Z",
            "end_ts": "2026-06-22T12:06:00Z",
            "start_block": 88900005,
            "end_block": 88900030,
        }
    ]


def _external_signal_events(*, run_id: int, source: str) -> list[dict[str, Any]]:
    return [
        {
            "run_id": run_id,
            "signal_id": "fixture-signal-news-001",
            "source": source,
            "event_type": "external_signal",
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-france",
            "token_side": "YES",
            "observed_at": "2026-06-22T11:59:55Z",
            "observed_block": 88900000,
            "latency_seconds": "5",
            "resolution_source": "fixture-news-source",
            "settlement_rule": "winner_resolves_yes",
            "price_to_beat_source": "orderfilled_block_close",
            "oracle_source": "polymarket",
            "payload": {
                "signal": "lineup_update",
                "confidence": "0.62",
                "expected_direction": "up",
            },
        },
        {
            "run_id": run_id,
            "signal_id": "fixture-signal-odds-002",
            "source": source,
            "event_type": "external_signal",
            "market_slug": "fixture-world-cup-winner",
            "token_id": "fixture-token-spain",
            "token_side": "YES",
            "observed_at": "2026-06-22T12:01:45Z",
            "observed_block": 88900008,
            "latency_seconds": "8",
            "resolution_source": "fixture-odds-source",
            "settlement_rule": "winner_resolves_yes",
            "price_to_beat_source": "orderfilled_block_close",
            "oracle_source": "polymarket",
            "payload": {
                "signal": "cross_market_move",
                "confidence": "0.54",
                "expected_direction": "flat",
            },
        },
    ]


def _order_state_events_from_plan(plan: Mapping[str, Any], *, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, order in enumerate(plan.get("orders") or [], start=1):
        if not isinstance(order, Mapping):
            continue
        if not _is_external_order_candidate(order):
            continue
        run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
        template = dict(order.get("event_template") or {})
        payload = dict(template.get("payload") or {})
        status = _live_status_for_order(order)
        filled_size = _decimal_or_zero(_first_present(order, "actual_fill_size", "filled_size", "expected_fill_size"))
        filled_notional = _decimal_or_zero(_first_present(order, "actual_fill_notional", "filled_notional", "expected_fill_notional"))
        fill_price = _fill_price(order, filled_size=filled_size, filled_notional=filled_notional)
        fee = _decimal_or_zero(order.get("fee_cost"))
        rebate = _decimal_or_zero(order.get("rebate_cost"))
        signed_notional = filled_notional if _side(order) == "SELL" else -filled_notional
        cash_delta = signed_notional - fee + rebate
        position_delta = -filled_size if _side(order) == "SELL" else filled_size
        event_time = f"2026-06-22T12:{index % 60:02d}:00Z"
        external_order_id = template.get("external_order_id") or f"{source}-{order.get('order_id')}"

        payload.update(
            {
                "live_status": status,
                "live_latency_seconds": _decimal_text(_decimal_or_zero(order.get("latency_seconds"))),
                "simulated_order_id": order.get("order_id"),
                "simulated_status": order.get("simulated_status"),
                "execution_source": order.get("execution_source"),
                "no_fill_reason": order.get("no_fill_reason"),
            }
        )
        if status in {"FILLED", "PARTIAL_FILLED"}:
            payload.update(
                {
                    "live_fill_price": _decimal_text(fill_price),
                    "live_fill_size": _decimal_text(filled_size),
                    "live_fee": _decimal_text(fee),
                    "live_rebate": _decimal_text(rebate),
                    "live_cash_delta": _decimal_text(cash_delta),
                    "live_position_delta": _decimal_text(position_delta),
                    "live_slippage": _decimal_text(_decimal_or_zero(order.get("slippage_cost"))),
                    "maker_taker": str(order.get("role") or "").lower() or "unknown",
                }
            )
        else:
            payload.update(
                {
                    "live_fill_price": "",
                    "live_fill_size": "",
                    "live_fee": _decimal_text(fee),
                    "live_rebate": _decimal_text(rebate),
                    "live_cash_delta": "0",
                    "live_position_delta": "0",
                }
            )

        template.update(
            {
                "run_id": run.get("run_id") or template.get("run_id"),
                "order_id": order.get("order_id") or template.get("order_id"),
                "external_order_id": external_order_id,
                "market_slug": order.get("market_slug") or run.get("market_slug") or template.get("market_slug"),
                "token_id": order.get("token_id") or template.get("token_id"),
                "token_side": order.get("token_side") or run.get("token_side") or template.get("token_side"),
                "event_time": event_time,
                "event_type": "order_state",
                "source": source,
                "api_order_status": status,
                "clob_order_status": status,
                "payload": payload,
            }
        )
        rows.append(template)
    return rows


def _cost_events_from_plan(plan: Mapping[str, Any], *, source: str) -> list[dict[str, Any]]:
    run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
    rows: list[dict[str, Any]] = []
    for index, order in enumerate(plan.get("orders") or [], start=1):
        if (
            not isinstance(order, Mapping)
            or not _is_external_order_candidate(order)
            or _live_status_for_order(order) not in {"FILLED", "PARTIAL_FILLED"}
        ):
            continue
        event_time = f"2026-06-22T12:{index % 60:02d}:00Z"
        for event_type, field in (("FEE", "fee_cost"), ("REBATE", "rebate_cost")):
            amount = _decimal_or_zero(order.get(field))
            if amount == 0:
                continue
            rows.append(_cost_event_from_order(order, run=run, source=source, event_type=event_type, amount=amount, observed_at=event_time))
    if not rows:
        first_filled = next(
            (
                order
                for order in plan.get("orders") or []
                if isinstance(order, Mapping)
                and _is_external_order_candidate(order)
                and _live_status_for_order(order) in {"FILLED", "PARTIAL_FILLED"}
            ),
            None,
        )
        if isinstance(first_filled, Mapping):
            rows.append(
                _cost_event_from_order(
                    first_filled,
                    run=run,
                    source=source,
                    event_type="FEE",
                    amount=Decimal("0"),
                    observed_at="2026-06-22T12:00:00Z",
                )
            )
    return rows


def _platform_incidents_from_plan(plan: Mapping[str, Any], *, source: str) -> list[dict[str, Any]]:
    run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
    return [
        {
            "incident_key": f"run-{run.get('run_id') or 'unknown'}-fixture-shadow-live-window",
            "source": source,
            "severity": "info",
            "component": "shadow_live_fixture",
            "title": "Run-specific fixture evidence window",
            "description": "Synthetic incident used only to verify run-specific fill-first external source plumbing.",
            "market_slug": run.get("market_slug"),
            "token_side": run.get("token_side"),
            "start_ts": "2026-06-22T12:00:00Z",
            "end_ts": "2026-06-22T12:59:00Z",
            "start_block": run.get("from_block"),
            "end_block": run.get("to_block"),
        }
    ]


def _external_signal_events_from_plan(plan: Mapping[str, Any], *, source: str) -> list[dict[str, Any]]:
    run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
    rows: list[dict[str, Any]] = []
    for index, order in enumerate(plan.get("orders") or [], start=1):
        if not isinstance(order, Mapping) or not _is_external_order_candidate(order):
            continue
        signal_x = _int_or_none(order.get("signal_x")) or _int_or_none(run.get("from_block")) or 0
        rows.append(
            {
                "run_id": run.get("run_id"),
                "signal_id": f"{run.get('run_id') or 'run'}-{order.get('order_id')}-signal",
                "source": source,
                "event_type": "external_signal",
                "market_slug": order.get("market_slug") or run.get("market_slug"),
                "token_id": order.get("token_id"),
                "token_side": order.get("token_side") or run.get("token_side"),
                "observed_at": f"2026-06-22T11:{index % 60:02d}:30Z",
                "observed_block": signal_x,
                "latency_seconds": _decimal_text(_decimal_or_zero(order.get("latency_seconds"))),
                "resolution_source": "run-fixture-signal-source",
                "settlement_rule": "market_resolution_rule_required",
                "price_to_beat_source": "orderfilled_block_close",
                "oracle_source": "polymarket",
                "payload": {
                    "simulated_order_id": order.get("order_id"),
                    "decision_price": order.get("decision_price") or order.get("requested_price"),
                    "requested_size": order.get("requested_size"),
                    "role": order.get("role"),
                    "no_fill_reason": order.get("no_fill_reason"),
                },
            }
        )
    return rows


def _cost_event_from_order(
    order: Mapping[str, Any],
    *,
    run: Mapping[str, Any],
    source: str,
    event_type: str,
    amount: Decimal,
    observed_at: str,
) -> dict[str, Any]:
    return {
        "run_id": run.get("run_id"),
        "cost_id": f"{run.get('run_id') or 'run'}-{order.get('order_id')}-{event_type.lower()}",
        "source": source,
        "market_slug": order.get("market_slug") or run.get("market_slug"),
        "token_id": order.get("token_id"),
        "token_side": order.get("token_side") or run.get("token_side"),
        "order_id": order.get("order_id"),
        "event_type": event_type,
        "observed_at": observed_at,
        "observed_block": order.get("submit_x") or order.get("signal_x") or run.get("from_block"),
        "amount": _decimal_text(amount),
        "currency": "USDC",
        "tx_hash": f"fixture-{run.get('run_id') or 'run'}-{order.get('order_id')}-{event_type.lower()}",
    }


def _fixture_env(
    order_state_path: Path,
    cost_path: Path,
    incident_path: Path,
    external_signal_path: Path,
    *,
    run_id: int,
    source_prefix: str,
) -> dict[str, str]:
    return {
        "ORDER_STATE_INPUT": str(order_state_path),
        "ORDER_STATE_SOURCE": f"{source_prefix}-order-state",
        "ORDER_STATE_RUN_ID": str(run_id),
        "ORDER_STATE_KEY": f"{source_prefix}-order-state-events",
        "COST_EVENTS_INPUT": str(cost_path),
        "COST_EVENTS_SOURCE": f"{source_prefix}-wallet",
        "COST_EVENTS_RUN_ID": str(run_id),
        "COST_EVENTS_STATE_KEY": f"{source_prefix}-real-cost-events",
        "PLATFORM_INCIDENTS_INPUT": str(incident_path),
        "PLATFORM_INCIDENTS_SOURCE": f"{source_prefix}-ops",
        "PLATFORM_INCIDENTS_STATE_KEY": f"{source_prefix}-platform-incidents",
        "EXTERNAL_SIGNAL_INPUT": str(external_signal_path),
        "EXTERNAL_SIGNAL_SOURCE": f"{source_prefix}-signals",
        "EXTERNAL_SIGNAL_RUN_ID": str(run_id),
        "EXTERNAL_SIGNAL_STATE_KEY": f"{source_prefix}-external-signals",
    }


def _write_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_env_file(path: Path, values: Mapping[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in values.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _live_status_for_order(order: Mapping[str, Any]) -> str:
    simulated = str(order.get("simulated_status") or "").upper()
    filled_size = _decimal_or_zero(_first_present(order, "actual_fill_size", "filled_size", "expected_fill_size"))
    requested_size = _decimal_or_zero(order.get("requested_size"))
    if filled_size > 0:
        if requested_size > 0 and filled_size < requested_size:
            return "PARTIAL_FILLED"
        return "FILLED"
    if simulated in {"NO_FILL", "REJECTED", "FAILED", "EXPIRED", "CANCELED", "CANCELLED"}:
        return "REJECTED" if simulated == "REJECTED" else "NO_FILL"
    if "FILL" in simulated:
        return "FILLED"
    return "NO_FILL"


def _is_external_order_candidate(order: Mapping[str, Any]) -> bool:
    execution_source = str(order.get("execution_source") or "").lower()
    order_type = str(order.get("order_type") or "").upper()
    if execution_source in {"settlement_payoff", "force_close", "settlement"}:
        return False
    if order_type in {"SETTLEMENT", "REDEEM", "PAYOUT"}:
        return False
    return True


def _fill_price(order: Mapping[str, Any], *, filled_size: Decimal, filled_notional: Decimal) -> Decimal:
    explicit = _decimal_or_zero(order.get("avg_fill_price"))
    if explicit > 0:
        return explicit
    if filled_size > 0 and filled_notional > 0:
        return filled_notional / filled_size
    fallback = _decimal_or_zero(order.get("requested_price") or order.get("decision_price"))
    return fallback


def _side(order: Mapping[str, Any]) -> str:
    return str(order.get("side") or "BUY").upper()


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_zero(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
