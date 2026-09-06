"""Local discovery/import helpers for fill-first external source files."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from quant.backtest.cost_calibration import normalize_real_cost_event, upsert_real_cost_events
from quant.backtest.external_signals import normalize_external_signal_event, upsert_external_signal_events
from quant.backtest.external_source_state import upsert_external_source_import_state
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events
from quant.backtest.platform_incidents import normalize_platform_incident, upsert_platform_incidents
from quant.backtest.shadow_live_validation import validate_shadow_live_order_events


REAL_COST_EVENTS = "real_cost_events"
PLATFORM_INCIDENTS = "platform_incidents"
REAL_ORDER_STATE_EVENTS = "real_order_state_events"
EXTERNAL_SIGNAL_EVENTS = "external_signal_events"
SUPPORTED_KINDS = {REAL_COST_EVENTS, PLATFORM_INCIDENTS, REAL_ORDER_STATE_EVENTS, EXTERNAL_SIGNAL_EVENTS}

SKIP_DIRS = {".git", ".pytest_cache", ".mypy_cache", "__pycache__", "node_modules", ".venv", "venv"}
SUPPORTED_SUFFIXES = {".json", ".jsonl"}

COST_NAME_TOKENS = (
    "real_cost",
    "cost_event",
    "cost-events",
    "wallet_cost",
    "wallet-cost",
    "wallet_ledger",
    "wallet-ledger",
    "fee_event",
    "fee-events",
    "rebate",
    "gas_cost",
)
INCIDENT_NAME_TOKENS = (
    "platform_incident",
    "platform-incident",
    "incident",
    "outage",
    "maintenance",
    "clob_status",
    "gamma_status",
    "service_status",
)
ORDER_STATE_NAME_TOKENS = (
    "order_state",
    "order-state",
    "real_order",
    "real-order",
    "shadow_live",
    "shadow-live",
    "live_shadow",
    "live-shadow",
    "clob_order",
    "clob-order",
)
EXTERNAL_SIGNAL_NAME_TOKENS = (
    "external_signal",
    "external-signal",
    "signal_event",
    "signal-events",
    "strategy_signal",
    "strategy-signal",
    "world_signal",
    "world-signal",
    "news_signal",
    "news-signal",
)

COST_ARRAY_KEYS = ("events", "items", "data")
INCIDENT_ARRAY_KEYS = ("incidents", "items", "data")
ORDER_STATE_ARRAY_KEYS = ("events", "order_state_events", "real_order_state_events", "event_templates", "items", "data")
EXTERNAL_SIGNAL_ARRAY_KEYS = ("external_signal_events", "external_signals", "signals", "signal_events", "events", "items", "data")


@dataclass(frozen=True)
class ExternalSourceCandidate:
    kind: str
    path: Path
    reason: str

    def as_dict(self, *, base: Path | None = None) -> dict[str, str]:
        return {
            "kind": self.kind,
            "path": _display_path(self.path, base=base),
            "reason": self.reason,
        }


def default_external_source_roots(project_root: Path) -> list[Path]:
    root = Path(project_root)
    return [root / "runtime_outputs", root / "exports", root / "data"]


def discover_external_source_files(
    roots: Iterable[Path],
    *,
    max_depth: int = 5,
    max_files_per_kind: int = 50,
) -> list[ExternalSourceCandidate]:
    candidates: list[ExternalSourceCandidate] = []
    counts = {REAL_COST_EVENTS: 0, PLATFORM_INCIDENTS: 0, REAL_ORDER_STATE_EVENTS: 0, EXTERNAL_SIGNAL_EVENTS: 0}
    seen: set[Path] = set()
    for root in roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        for path in _iter_source_files(root_path, max_depth=max_depth):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            candidate = classify_external_source_file(path)
            if candidate is None:
                continue
            if counts[candidate.kind] >= max_files_per_kind:
                continue
            candidates.append(candidate)
            counts[candidate.kind] += 1
    return sorted(candidates, key=lambda item: (item.kind, str(item.path)))


def classify_external_source_file(path: Path) -> ExternalSourceCandidate | None:
    name = path.name.lower()
    if path.suffix.lower() not in SUPPORTED_SUFFIXES:
        return None
    if any(token in name for token in ORDER_STATE_NAME_TOKENS):
        return ExternalSourceCandidate(REAL_ORDER_STATE_EVENTS, path, "filename")
    if any(token in name for token in EXTERNAL_SIGNAL_NAME_TOKENS):
        return ExternalSourceCandidate(EXTERNAL_SIGNAL_EVENTS, path, "filename")
    if any(token in name for token in INCIDENT_NAME_TOKENS):
        return ExternalSourceCandidate(PLATFORM_INCIDENTS, path, "filename")
    if any(token in name for token in COST_NAME_TOKENS):
        return ExternalSourceCandidate(REAL_COST_EVENTS, path, "filename")
    sample = _sample_records(path, limit=3)
    if not sample:
        return None
    if any(_looks_like_order_state_event(row) for row in sample):
        return ExternalSourceCandidate(REAL_ORDER_STATE_EVENTS, path, "payload")
    if any(_looks_like_external_signal(row) for row in sample):
        return ExternalSourceCandidate(EXTERNAL_SIGNAL_EVENTS, path, "payload")
    if any(_looks_like_incident(row) for row in sample):
        return ExternalSourceCandidate(PLATFORM_INCIDENTS, path, "payload")
    if any(_looks_like_cost_event(row) for row in sample):
        return ExternalSourceCandidate(REAL_COST_EVENTS, path, "payload")
    return None


def preview_external_source_import(
    candidates: Sequence[ExternalSourceCandidate],
    *,
    source_prefix: str = "local-discovery",
    base: Path | None = None,
    max_preview_rows: int = 5,
) -> dict[str, Any]:
    return process_external_source_candidates(
        candidates,
        source_prefix=source_prefix,
        base=base,
        max_preview_rows=max_preview_rows,
        conn=None,
        write=False,
    )


def import_external_source_candidates(
    conn: Any,
    candidates: Sequence[ExternalSourceCandidate],
    *,
    source_prefix: str = "local-discovery",
    state_prefix: str = "local-discovery",
    base: Path | None = None,
    max_preview_rows: int = 5,
) -> dict[str, Any]:
    return process_external_source_candidates(
        candidates,
        source_prefix=source_prefix,
        state_prefix=state_prefix,
        base=base,
        max_preview_rows=max_preview_rows,
        conn=conn,
        write=True,
    )


def process_external_source_candidates(
    candidates: Sequence[ExternalSourceCandidate],
    *,
    source_prefix: str,
    base: Path | None = None,
    max_preview_rows: int = 5,
    conn: Any | None = None,
    write: bool = False,
    state_prefix: str = "local-discovery",
) -> dict[str, Any]:
    summary = _empty_summary(write=write)
    for candidate in candidates:
        item = _candidate_summary(candidate, base=base)
        try:
            records = load_external_source_records(candidate.path, candidate.kind)
            item["records_read"] = len(records)
            source = _source_label(source_prefix, candidate, base=base)
            item["source"] = source
            if candidate.kind == REAL_COST_EVENTS:
                normalized = [normalize_real_cost_event(row, source=source) for row in records]
                item["preview"] = normalized[:max(0, int(max_preview_rows))]
                if write and conn is not None:
                    item["rows_written"] = upsert_real_cost_events(conn, normalized)
            elif candidate.kind == PLATFORM_INCIDENTS:
                normalized = [normalize_platform_incident(row, source=source) for row in records]
                item["preview"] = normalized[:max(0, int(max_preview_rows))]
                if write and conn is not None:
                    item["rows_written"] = upsert_platform_incidents(conn, normalized)
            elif candidate.kind == REAL_ORDER_STATE_EVENTS:
                normalized = [normalize_order_state_event(row, source=source) for row in records]
                validation = validate_shadow_live_order_events(normalized)
                item["validation_status"] = validation["status"]
                item["validation_errors"] = validation["error_count"]
                item["validation_warnings"] = validation["warning_count"]
                item["calibration_ready_count"] = validation["calibration_ready_count"]
                item["preview"] = normalized[:max(0, int(max_preview_rows))]
                if write and conn is not None:
                    item["rows_written"] = upsert_real_order_state_events(conn, normalized)
            elif candidate.kind == EXTERNAL_SIGNAL_EVENTS:
                normalized = [normalize_external_signal_event(row, source=source) for row in records]
                item["preview"] = normalized[:max(0, int(max_preview_rows))]
                item["missing_required_field_count"] = _external_signal_missing_required_field_count(normalized)
                if write and conn is not None:
                    item["rows_written"] = upsert_external_signal_events(conn, normalized)
            else:
                raise ValueError(f"unsupported external source kind: {candidate.kind}")
            item["status"] = "ready" if records else "review"
            if write and conn is not None:
                upsert_external_source_import_state(
                    conn,
                    state_key=_state_key(state_prefix, candidate, base=base),
                    source_type=candidate.kind,
                    source=source,
                    endpoint=str(candidate.path),
                    params={"discovered_by": "local_file_discovery", "reason": candidate.reason},
                    last_payload_count=len(records),
                    last_rows_written=int(item.get("rows_written") or 0),
                    last_error=None,
                )
        except Exception as exc:  # pragma: no cover - defensive path for malformed local files
            item["status"] = "review"
            item["error"] = str(exc)
            if write and conn is not None:
                upsert_external_source_import_state(
                    conn,
                    state_key=_state_key(state_prefix, candidate, base=base),
                    source_type=candidate.kind,
                    source=item.get("source") or source_prefix,
                    endpoint=str(candidate.path),
                    params={"discovered_by": "local_file_discovery", "reason": candidate.reason},
                    last_payload_count=0,
                    last_rows_written=0,
                    last_error=str(exc),
                )
        _add_item(summary, item)
    summary["status"] = _summary_status(summary)
    return summary


def load_external_source_records(path: Path, kind: str) -> list[dict[str, Any]]:
    if kind not in SUPPORTED_KINDS:
        raise ValueError(f"unsupported external source kind: {kind}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".jsonl":
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    payload = json.loads(text)
    if kind == REAL_COST_EVENTS:
        array_keys = COST_ARRAY_KEYS
    elif kind == PLATFORM_INCIDENTS:
        array_keys = INCIDENT_ARRAY_KEYS
    elif kind == REAL_ORDER_STATE_EVENTS:
        array_keys = ORDER_STATE_ARRAY_KEYS
    elif kind == EXTERNAL_SIGNAL_EVENTS:
        array_keys = EXTERNAL_SIGNAL_ARRAY_KEYS
    else:
        raise ValueError(f"unsupported external source kind: {kind}")
    return _records_from_payload(payload, array_keys=array_keys, source=str(path))


def external_source_discovery_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"status: {report.get('status')}",
        f"write: {report.get('write')}",
        f"files: {report.get('file_count', 0)}",
        f"records: {report.get('records_read', 0)}",
        f"rows_written: {report.get('rows_written', 0)}",
        "",
        "| kind | file | status | records | rows_written | validation | reason | error |",
        "| --- | --- | --- | ---: | ---: | --- | --- | --- |",
    ]
    for item in report.get("items", []):
        lines.append(
            "| {kind} | `{path}` | {status} | {records} | {written} | {validation} | {reason} | {error} |".format(
                kind=item.get("kind") or "",
                path=item.get("path") or "",
                status=item.get("status") or "",
                records=item.get("records_read") or 0,
                written=item.get("rows_written") or 0,
                validation=item.get("validation_status") or "",
                reason=item.get("reason") or "",
                error=item.get("error") or "",
            )
        )
    return "\n".join(lines)


def _iter_source_files(root: Path, *, max_depth: int) -> Iterable[Path]:
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            continue
        try:
            children = sorted(current.iterdir(), key=lambda path: path.name)
        except OSError:
            continue
        for child in children:
            if child.is_dir():
                if child.name in SKIP_DIRS:
                    continue
                stack.append((child, depth + 1))
            elif child.is_file() and child.suffix.lower() in SUPPORTED_SUFFIXES:
                yield child


def _sample_records(path: Path, *, limit: int) -> list[dict[str, Any]]:
    try:
        if path.suffix.lower() == ".jsonl":
            rows: list[dict[str, Any]] = []
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    parsed = json.loads(line)
                    if isinstance(parsed, Mapping):
                        rows.append(dict(parsed))
                    if len(rows) >= limit:
                        break
            return rows
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        parsed = json.loads(text)
        if isinstance(parsed, Mapping):
            for key in ("events", "incidents", "external_signal_events", "external_signals", "signals", "signal_events", "items", "data"):
                if isinstance(parsed.get(key), list):
                    return [dict(row) for row in parsed[key][:limit] if isinstance(row, Mapping)]
            return [dict(parsed)]
        if isinstance(parsed, list):
            return [dict(row) for row in parsed[:limit] if isinstance(row, Mapping)]
    except Exception:
        return []
    return []


def _records_from_payload(payload: Any, *, array_keys: Sequence[str], source: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in array_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(item) for item in value if isinstance(item, Mapping)]
        return [dict(payload)]
    raise ValueError(f"unsupported input JSON shape in {source}")


def _looks_like_cost_event(row: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in row.keys()}
    has_amount = bool(keys & {"amount", "cost", "fee", "rebate", "gas", "value"})
    has_cost_type = bool(keys & {"cost_id", "costid", "event_type", "eventtype", "cost_type", "costtype", "tx_hash", "txhash"})
    return has_amount and has_cost_type


def _looks_like_incident(row: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in row.keys()}
    has_window = bool(keys & {"start_ts", "startts", "start_time", "starttime", "start", "from_ts", "fromts", "start_block", "startblock"})
    has_incident = bool(keys & {"incident_key", "incidentkey", "severity", "component", "service", "system", "title", "summary"})
    return has_window and has_incident


def _looks_like_order_state_event(row: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in row.keys()}
    has_order = bool(keys & {"order_id", "orderid", "external_order_id", "externalorderid", "clob_order_id", "cloborderid", "order_hash", "orderhash"})
    has_state = bool(keys & {"api_order_status", "apiorderstatus", "chain_order_status", "chainorderstatus", "clob_order_status", "cloborderstatus", "submit_status", "submitstatus", "cancel_status", "cancelstatus", "live_status", "livestatus", "event_type", "eventtype"})
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        payload_keys = {str(key).lower() for key in payload.keys()}
        has_state = has_state or bool(payload_keys & {"live_status", "livestatus", "live_fill_price", "livefillprice", "live_fill_size", "livefillsize"})
    return has_order and has_state


def _looks_like_external_signal(row: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in row.keys()}
    has_observation = bool(keys & {"observed_at", "observedat", "timestamp", "time", "observed_block", "observedblock", "block_number", "blocknumber"})
    has_signal_identity = bool(keys & {"signal_id", "signalid", "event_id", "eventid", "payload_hash", "payloadhash"})
    has_signal_context = bool(
        keys
        & {
            "latency_seconds",
            "latencyseconds",
            "settlement_rule",
            "settlementrule",
            "resolution_source",
            "resolutionsource",
            "price_to_beat_source",
            "pricetobeatsource",
            "oracle_source",
            "oraclesource",
            "market_slug",
            "marketslug",
            "token_id",
            "tokenid",
        }
    )
    event_type = str(row.get("event_type") or row.get("eventType") or row.get("type") or "").lower()
    if "signal" in event_type:
        has_signal_context = True
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        payload_keys = {str(key).lower() for key in payload.keys()}
        has_signal_context = has_signal_context or bool(payload_keys & {"signal", "confidence", "edge", "reason", "payload_hash"})
    return has_observation and has_signal_identity and has_signal_context


def _external_signal_missing_required_field_count(rows: Sequence[Mapping[str, Any]]) -> int:
    required = ("observed_at", "source", "latency_seconds", "payload_hash")
    return sum(1 for row in rows for field in required if row.get(field) in (None, ""))


def _empty_summary(*, write: bool) -> dict[str, Any]:
    return {
        "status": "unknown",
        "write": bool(write),
        "file_count": 0,
        "records_read": 0,
        "rows_written": 0,
        "kind_counts": {REAL_COST_EVENTS: 0, PLATFORM_INCIDENTS: 0, REAL_ORDER_STATE_EVENTS: 0, EXTERNAL_SIGNAL_EVENTS: 0},
        "items": [],
    }


def _candidate_summary(candidate: ExternalSourceCandidate, *, base: Path | None) -> dict[str, Any]:
    return {
        "kind": candidate.kind,
        "path": _display_path(candidate.path, base=base),
        "reason": candidate.reason,
        "records_read": 0,
        "rows_written": 0,
        "status": "unknown",
        "error": None,
    }


def _add_item(summary: dict[str, Any], item: Mapping[str, Any]) -> None:
    summary["items"].append(dict(item))
    summary["file_count"] += 1
    summary["records_read"] += int(item.get("records_read") or 0)
    summary["rows_written"] += int(item.get("rows_written") or 0)
    if item.get("kind") in summary["kind_counts"]:
        summary["kind_counts"][item["kind"]] += 1


def _summary_status(summary: Mapping[str, Any]) -> str:
    items = list(summary.get("items") or [])
    if not items:
        return "unknown"
    if any(item.get("status") == "review" for item in items):
        return "review"
    return "ready"


def _source_label(prefix: str, candidate: ExternalSourceCandidate, *, base: Path | None) -> str:
    return f"{prefix}:{candidate.kind}:{_display_path(candidate.path, base=base)}"


def _state_key(prefix: str, candidate: ExternalSourceCandidate, *, base: Path | None) -> str:
    display = _display_path(candidate.path, base=base).replace("/", ":")
    return f"{prefix}:{candidate.kind}:{display}"


def _display_path(path: Path, *, base: Path | None) -> str:
    if base is not None:
        try:
            return path.resolve().relative_to(base.resolve()).as_posix()
        except ValueError:
            pass
    return str(path)
