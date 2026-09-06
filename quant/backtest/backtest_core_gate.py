"""Historical backtest-core quality gate.

This gate is intentionally narrower than the fill-first production/paper gate.
It answers whether the historical backtest framework is structurally healthy:
data replay, execution model, ledger, artifacts, performance reports, and
optional latest-run fill evidence.  It does not judge paper/live readiness,
external order-state coverage, production launch safety, or real order APIs.
"""

from __future__ import annotations

from dataclasses import dataclass
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from quant.backtest.fill_evidence import build_fill_evidence_validation_report
from quant.backtest.fill_first_quality_gate import _current_schema_artifact_fixture_inputs
from quant.backtest.fill_first_readiness import MISSING, READY, REVIEW, build_fill_first_readiness_report
from quant.backtest.phase1_doc_alignment import build_phase1_doc_alignment_report
from quant.backtest.run_artifacts import (
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
)


FAIL = "fail"
UNKNOWN = "unknown"

CommandRunner = Callable[[Sequence[str], Path], dict[str, Any]]

EXCLUDED_NAME_TOKENS = (
    "frontend",
    "paper",
    "live",
    "shadow",
    "external",
    "production",
    "strategy activation",
    "strategy runner",
    "guarded",
    "order execution",
    "real order",
    "order-state",
    "wallet",
    "incident",
    "env ",
    "env-file",
    "bootstrap",
    "systemd",
    "promotion gate",
    "calibration source",
    "configured source",
)

CORE_ARTIFACT_CHECK_NAMES = {
    "run status",
    "parameter snapshot",
    "reproducibility report",
    "materialized replay cache",
    "raw orderfilled replay contract",
    "historical l2 alignment report",
    "block/window",
    "data quality artifact",
    "fill quality artifact",
    "order lifecycle",
    "execution semantics report",
    "fill probability evidence report",
    "maker/taker execution report",
    "maker queue uncertainty report",
    "latency profile report",
    "slippage regime report",
    "cashflow ledger",
    "execution/ledger parity report",
    "ledger cashflow validation",
    "execution regime report",
    "regime coverage report",
    "tail risk report",
    "prediction quality report",
    "performance score report",
    "market lifecycle report",
    "settlement compatibility report",
    "event-level risk report",
    "event stream contract",
    "joint replay plan",
    "joint replay execution",
    "quality metrics",
}


@dataclass(frozen=True)
class CoreGateCheck:
    name: str
    status: str
    detail: str
    evidence: str
    payload: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "evidence": self.evidence,
            "payload": dict(self.payload or {}),
        }


def build_backtest_core_gate_report(
    project_root: Path,
    *,
    conn: Any | None = None,
    check_db: bool = False,
    include_latest_run_artifact: bool = False,
    include_fill_evidence_validation: bool = False,
    include_pytest: bool = False,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    checks = [
        _core_readiness_subset_check(root, conn=conn, check_db=check_db),
        _phase1_doc_alignment_check(root),
        _current_schema_core_artifact_fixture_check(),
        _generic_replay_sql_contract_check(),
        _trade_replay_sql_contract_check(),
        _pmxt_l2_replay_fixture_check(),
    ]
    if include_latest_run_artifact:
        checks.append(_latest_run_core_artifact_check(conn))
    if include_fill_evidence_validation:
        checks.append(_latest_fill_evidence_validation_check(conn))
    if include_pytest:
        checks.append(
            _command_check(
                "pytest backtest-core subset",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "quant/backtest/tests/test_event_stream.py",
                    "quant/backtest/tests/test_limit_replay_raw_events.py",
                    "quant/backtest/tests/test_orderfilled_lob_model.py",
                    "quant/backtest/tests/test_block_bar_ohlcv.py",
                    "quant/backtest/tests/test_block_replay_store.py",
                ],
                cwd=root,
                evidence="quant/backtest/tests",
                command_runner=command_runner,
            )
        )
    status = aggregate_core_gate_status(check.status for check in checks)
    return {
        "status": status,
        "scope": (
            "backtest-core: historical data replay, execution model, ledger, artifact, "
            "performance, LOB/PMXT historical enhancement; paper/live/production excluded"
        ),
        "ready_count": sum(1 for check in checks if check.status == READY),
        "review_count": sum(1 for check in checks if check.status in {REVIEW, UNKNOWN}),
        "fail_count": sum(1 for check in checks if check.status in {FAIL, MISSING}),
        "checks": [check.as_dict() for check in checks],
        "excluded_domains": [
            "paper/live order-state coverage",
            "external wallet/cost/incident/signal sources",
            "production preflight and launch safety",
            "real order API adapter readiness",
        ],
        "next_actions": core_gate_next_actions(checks),
    }


def aggregate_core_gate_status(statuses: Sequence[str] | Any) -> str:
    values = set(statuses)
    if FAIL in values or MISSING in values:
        return FAIL
    if REVIEW in values or UNKNOWN in values:
        return REVIEW
    return READY


def core_gate_next_actions(checks: Sequence[CoreGateCheck]) -> list[str]:
    actions: list[str] = []
    for check in checks:
        if check.status in {FAIL, MISSING}:
            actions.append(f"Fix backtest-core gate: {check.name} ({check.detail})")
        elif check.status in {REVIEW, UNKNOWN}:
            actions.append(f"Review backtest-core gate: {check.name} ({check.detail})")
    if not actions:
        actions.append("Backtest-core gate is ready; continue strengthening raw replay, execution realism, ledger, LOB/PMXT, and portfolio replay.")
    return actions


def core_gate_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Backtest Core Gate: {report.get('status')}",
        "",
        f"Scope: {report.get('scope')}",
        "",
        f"- ready: {report.get('ready_count', 0)}",
        f"- review: {report.get('review_count', 0)}",
        f"- fail: {report.get('fail_count', 0)}",
        "",
        "| Gate | Status | Detail | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.get("checks", []):
        lines.append(
            "| {name} | {status} | {detail} | `{evidence}` |".format(
                name=check.get("name", ""),
                status=check.get("status", ""),
                detail=str(check.get("detail", "")).replace("|", "\\|"),
                evidence=check.get("evidence", ""),
            )
        )
    lines.extend(["", "## Excluded"])
    lines.extend(f"- {item}" for item in report.get("excluded_domains", []))
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _core_readiness_subset_check(root: Path, *, conn: Any | None, check_db: bool) -> CoreGateCheck:
    report = build_fill_first_readiness_report(root, conn=conn, check_db=check_db)
    filtered, excluded = _filter_report_checks(report.get("checks") or [], allow_names=None)
    status = aggregate_core_gate_status(str(check.get("status") or UNKNOWN) for check in filtered)
    if not filtered:
        status = MISSING
    detail = f"core_checks={len(filtered)} excluded={len(excluded)} source_status={report.get('status')}"
    return CoreGateCheck(
        "fill-first core readiness subset",
        status,
        detail,
        "quant.backtest.fill_first_readiness",
        {
            "source_status": report.get("status"),
            "included_check_names": [check.get("name") for check in filtered],
            "excluded_check_names": [check.get("name") for check in excluded],
            "checks": filtered,
        },
    )


def _phase1_doc_alignment_check(root: Path) -> CoreGateCheck:
    report = build_phase1_doc_alignment_report(root)
    status = READY if report.get("status") == READY else FAIL if report.get("status") == MISSING else REVIEW
    detail = f"status={report.get('status')} ready={report.get('ready_count', 0)} missing={report.get('missing_count', 0)}"
    return CoreGateCheck("phase1 doc alignment", status, detail, "docs/量化/phase1.md", report)


def _current_schema_core_artifact_fixture_check() -> CoreGateCheck:
    try:
        source_report = build_backtest_run_artifact_report(_current_schema_artifact_fixture_inputs())
    except Exception as exc:  # pragma: no cover - defensive gate path
        return CoreGateCheck("current schema core artifact fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.run_artifacts", {})
    core_report = _core_artifact_report(source_report)
    materialized = source_report.get("materialized_cache_report")
    if isinstance(materialized, dict):
        contract = materialized.get("data_access_contract")
        if isinstance(contract, dict):
            core_report["data_access_contract"] = dict(contract)
    status = aggregate_core_gate_status(str(check.get("status") or UNKNOWN) for check in core_report["checks"])
    if not core_report["checks"]:
        status = MISSING
    detail = f"core_checks={len(core_report['checks'])} excluded={len(core_report['excluded_check_names'])} source_status={source_report.get('status')}"
    return CoreGateCheck("current schema core artifact fixture", status, detail, "quant.backtest.run_artifacts", core_report)


def _pmxt_l2_replay_fixture_check() -> CoreGateCheck:
    try:
        from scripts.validate_pmxt_l2_raw import (
            TokenSelection,
            build_pmxt_l2_replay_report,
            build_pmxt_orderfilled_alignment_report,
            orderfilled_l2_alignment_report_payload,
        )

        selection = TokenSelection(
            condition_id="condition-a",
            token_id="token-a",
            token_side="YES",
            market_id=123,
            market_slug="demo-market",
        )
        rows = [
            {
                "timestamp": 1_772_000_000_000,
                "market": "condition-a",
                "event_type": "book",
                "asset_id": "token-a",
                "bids": '[{"price": "0.49", "size": "100"}]',
                "asks": '[{"price": "0.51", "size": "90"}]',
                "price": None,
                "size": None,
                "side": None,
            },
            {
                "timestamp": 1_772_000_000_100,
                "market": "condition-a",
                "event_type": "price_change",
                "asset_id": "token-a",
                "bids": None,
                "asks": None,
                "price": "0.50",
                "size": "12",
                "side": "BUY",
            },
        ]
        report = build_pmxt_l2_replay_report(rows, schema_kind="fixed", selection=selection)
        alignment = build_pmxt_orderfilled_alignment_report(
            rows,
            schema_kind="fixed",
            selection=selection,
            orderfilled_rows=[
                {
                    "event_ts_ms": 1_772_000_000_200,
                    "market_id": 123,
                    "token_id": "token-a",
                    "block_number": 88_000_001,
                    "transaction_index": 2,
                    "log_index": 7,
                    "tx_hash": "0xfixture",
                    "trade_price": "0.505",
                    "size": "10",
                    "side_code": 1,
                }
            ],
            max_lag_ms=1_000,
        )
    except Exception as exc:  # pragma: no cover - defensive gate path
        return CoreGateCheck("pmxt l2 replay fixture", FAIL, f"fixture raised: {exc}", "scripts/validate_pmxt_l2_raw.py", {})
    required_ready = (
        report.status == READY
        and report.snapshot_events >= 1
        and report.price_change_events >= 1
        and report.upsert_delta_count >= 1
        and report.final_book.get("best_bid") == "0.50"
        and alignment.status == READY
        and alignment.aligned_count == 1
    )
    status = READY if required_ready else FAIL
    detail = (
        f"status={report.status} snapshots={report.snapshot_events} price_changes={report.price_change_events} "
        f"deltas={report.l2_delta_count} upserts={report.upsert_delta_count} "
        f"alignment={alignment.status} aligned={alignment.aligned_count}/{alignment.orderfilled_rows_matched}"
    )
    return CoreGateCheck(
        "pmxt l2 replay fixture",
        status,
        detail,
        "scripts/validate_pmxt_l2_raw.py",
        {**report.__dict__, "orderfilled_alignment": orderfilled_l2_alignment_report_payload(alignment)},
    )


def _generic_replay_sql_contract_check() -> CoreGateCheck:
    try:
        from quant.backtest.runners.generic_replay_benchmark import build_generic_replay_sql_contract_report

        report = build_generic_replay_sql_contract_report()
    except Exception as exc:  # pragma: no cover - defensive gate path
        return CoreGateCheck("generic replay sql contract", FAIL, f"contract raised: {exc}", "quant.backtest.runners.generic_replay_benchmark", {})
    status = READY if report.get("status") == READY else FAIL
    checks = report.get("checks") if isinstance(report.get("checks"), Mapping) else {}
    detail = (
        f"status={report.get('status')} token_pair_prewhere={checks.get('raw_pair_prewhere')} "
        f"block_replay={checks.get('block_replay_grouped')} vwap={checks.get('block_replay_vwap')} "
        f"side_volume={checks.get('block_replay_side_volume')}"
    )
    return CoreGateCheck("generic replay sql contract", status, detail, "quant.backtest.runners.generic_replay_benchmark", report)


def _trade_replay_sql_contract_check() -> CoreGateCheck:
    try:
        from quant.backtest.runners.trade_replay_store import build_trade_replay_sql_contract_report

        report = build_trade_replay_sql_contract_report()
    except Exception as exc:  # pragma: no cover - defensive gate path
        return CoreGateCheck("trade replay sql contract", FAIL, f"contract raised: {exc}", "quant.backtest.runners.trade_replay_store", {})
    status = READY if report.get("status") == READY else FAIL
    checks = report.get("checks") if isinstance(report.get("checks"), Mapping) else {}
    detail = (
        f"status={report.get('status')} pair_prewhere={checks.get('loader_pair_prewhere')} "
        f"tick_ordered={checks.get('loader_tick_ordered')} canonical={checks.get('canonical_fill_key_persisted')} "
        f"attribution={checks.get('maker_taker_side_persisted')}"
    )
    return CoreGateCheck("trade replay sql contract", status, detail, "quant.backtest.runners.trade_replay_store", report)


def _latest_run_core_artifact_check(conn: Any | None) -> CoreGateCheck:
    if conn is None:
        return CoreGateCheck(
            "latest run core artifact audit",
            REVIEW,
            "database not checked; pass --check-db",
            "scripts/audit_backtest_run_artifacts.py",
            {},
        )
    run_id = load_latest_fill_first_backtest_run_id(conn)
    if run_id is None:
        return CoreGateCheck(
            "latest run core artifact audit",
            REVIEW,
            "no fill-first run found",
            "scripts/audit_backtest_run_artifacts.py",
            {"status": MISSING, "reason": "no_fill_first_backtest_runs"},
        )
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    source_report = build_backtest_run_artifact_report(inputs, run_id=run_id)
    core_report = _core_artifact_report(source_report)
    status = aggregate_core_gate_status(str(check.get("status") or UNKNOWN) for check in core_report["checks"])
    if not core_report["checks"]:
        status = MISSING
    detail = f"run_id={run_id} core_checks={len(core_report['checks'])} excluded={len(core_report['excluded_check_names'])} source_status={source_report.get('status')}"
    return CoreGateCheck("latest run core artifact audit", status, detail, "scripts/audit_backtest_run_artifacts.py", core_report)


def _latest_fill_evidence_validation_check(conn: Any | None) -> CoreGateCheck:
    if conn is None:
        return CoreGateCheck(
            "latest fill evidence validation",
            REVIEW,
            "database not checked; pass --check-db",
            "scripts/validate_fill_evidence.py",
            {},
        )
    run_id = load_latest_fill_first_backtest_run_id(conn)
    if run_id is None:
        return CoreGateCheck(
            "latest fill evidence validation",
            REVIEW,
            "no fill-first run found",
            "scripts/validate_fill_evidence.py",
            {"status": MISSING, "reason": "no_fill_first_backtest_runs"},
        )
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    report = build_fill_evidence_validation_report(list((inputs or {}).get("orders") or []))
    status = READY if report.get("status") == READY else REVIEW
    detail = (
        f"run_id={run_id} status={report.get('status')} filled={report.get('filled_count', 0)} "
        f"raw={report.get('raw_orderfilled_fill_count', 0)}"
    )
    return CoreGateCheck("latest fill evidence validation", status, detail, "scripts/validate_fill_evidence.py", report)


def _core_artifact_report(source_report: Mapping[str, Any]) -> dict[str, Any]:
    checks = list(source_report.get("checks") or [])
    filtered, excluded = _filter_report_checks(checks, allow_names=CORE_ARTIFACT_CHECK_NAMES)
    status = aggregate_core_gate_status(str(check.get("status") or UNKNOWN) for check in filtered) if filtered else MISSING
    return {
        "status": status,
        "source_status": source_report.get("status"),
        "run_id": source_report.get("run_id"),
        "schema_version": source_report.get("schema_version"),
        "checks": filtered,
        "included_check_names": [check.get("name") for check in filtered],
        "excluded_check_names": [check.get("name") for check in excluded],
        "source_next_actions": list(source_report.get("next_actions") or []),
    }


def _filter_report_checks(
    checks: Sequence[Mapping[str, Any]],
    *,
    allow_names: set[str] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for check in checks:
        name = str(check.get("name") or "")
        if allow_names is not None:
            keep = name in allow_names
        else:
            keep = not _is_excluded_check_name(name)
        if keep:
            included.append(dict(check))
        else:
            excluded.append(dict(check))
    return included, excluded


def _is_excluded_check_name(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in EXCLUDED_NAME_TOKENS)


def _command_check(
    name: str,
    cmd: Sequence[str],
    *,
    cwd: Path,
    evidence: str,
    command_runner: CommandRunner | None = None,
) -> CoreGateCheck:
    try:
        if command_runner is not None:
            result = command_runner(cmd, cwd)
            returncode = int(result.get("returncode", 0))
            stdout = str(result.get("stdout", ""))
            stderr = str(result.get("stderr", ""))
        else:
            proc = subprocess.run(list(cmd), cwd=str(cwd), text=True, capture_output=True, check=False)
            returncode = proc.returncode
            stdout = proc.stdout
            stderr = proc.stderr
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return CoreGateCheck(name, FAIL, f"command raised: {exc}", evidence, {"cmd": list(cmd)})
    output = (stdout + "\n" + stderr).strip()
    status = READY if returncode == 0 else FAIL
    detail = "passed" if returncode == 0 else f"exit={returncode}"
    return CoreGateCheck(name, status, detail, evidence, {"cmd": list(cmd), "output_tail": output[-4000:]})
