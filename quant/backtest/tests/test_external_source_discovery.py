import json
from pathlib import Path

from quant.backtest.external_source_discovery import (
    EXTERNAL_SIGNAL_EVENTS,
    PLATFORM_INCIDENTS,
    REAL_COST_EVENTS,
    REAL_ORDER_STATE_EVENTS,
    discover_external_source_files,
    external_source_discovery_to_markdown,
    load_external_source_records,
    preview_external_source_import,
)


def test_discovers_cost_and_incident_files_by_name(tmp_path: Path) -> None:
    root = tmp_path / "runtime_outputs"
    root.mkdir()
    (root / "wallet_ledger.jsonl").write_text(
        json.dumps({"cost_id": "c1", "event_type": "gas", "amount": "0.12"}) + "\n",
        encoding="utf-8",
    )
    (root / "platform_incidents.json").write_text(
        json.dumps({"incidents": [{"incident_key": "i1", "severity": "warning", "start_ts": "2026-06-25T00:00:00Z"}]}),
        encoding="utf-8",
    )
    (root / "external_signal_events.jsonl").write_text(
        json.dumps({"signal_id": "s1", "observed_at": "2026-06-25T00:00:00Z", "latency_seconds": "2", "payload": {"signal": "news"}})
        + "\n",
        encoding="utf-8",
    )

    candidates = discover_external_source_files([root])

    assert [(item.kind, item.path.name) for item in candidates] == [
        (EXTERNAL_SIGNAL_EVENTS, "external_signal_events.jsonl"),
        (PLATFORM_INCIDENTS, "platform_incidents.json"),
        (REAL_COST_EVENTS, "wallet_ledger.jsonl"),
    ]


def test_discovers_sources_by_payload_shape(tmp_path: Path) -> None:
    root = tmp_path / "exports"
    root.mkdir()
    (root / "a.json").write_text(
        json.dumps({"events": [{"costId": "c1", "type": "fee", "amount": "1.5"}]}),
        encoding="utf-8",
    )
    (root / "b.jsonl").write_text(
        json.dumps({"id": "i1", "component": "clob_api", "start": "2026-06-25T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    (root / "c.json").write_text(
        json.dumps({"signals": [{"signal_id": "s1", "observed_at": "2026-06-25T00:01:00Z", "latency_seconds": "1", "payload": {"edge": "0.02"}}]}),
        encoding="utf-8",
    )

    candidates = discover_external_source_files([root])

    assert {item.kind for item in candidates} == {REAL_COST_EVENTS, PLATFORM_INCIDENTS, EXTERNAL_SIGNAL_EVENTS}


def test_discovers_shadow_live_order_state_files(tmp_path: Path) -> None:
    root = tmp_path / "runtime_outputs"
    root.mkdir()
    (root / "shadow_live_order_events.jsonl").write_text(
        json.dumps(
            {
                "run_id": 42,
                "order_id": "O-1",
                "external_order_id": "L-1",
                "event_time": "2026-06-25T12:00:00Z",
                "api_order_status": "filled",
                "payload": {
                    "live_status": "FILLED",
                    "live_fill_price": "0.53",
                    "live_fill_size": "10",
                    "live_fee": "0.01",
                    "live_rebate": "0",
                    "live_cash_delta": "-5.31",
                    "live_position_delta": "10",
                    "live_latency_seconds": "1.2",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    candidates = discover_external_source_files([root])
    report = preview_external_source_import(candidates, base=tmp_path)

    assert candidates[0].kind == REAL_ORDER_STATE_EVENTS
    assert report["status"] == "ready"
    assert report["kind_counts"][REAL_ORDER_STATE_EVENTS] == 1
    item = report["items"][0]
    assert item["validation_status"] == "ready"
    assert item["calibration_ready_count"] == 1
    assert item["preview"][0]["source"].startswith("local-discovery:real_order_state_events")


def test_preview_normalizes_discovered_records(tmp_path: Path) -> None:
    root = tmp_path / "runtime_outputs"
    root.mkdir()
    cost_path = root / "real_cost_events.json"
    incident_path = root / "incident_windows.jsonl"
    cost_path.write_text(json.dumps([{"cost_id": "c1", "event_type": "gas", "amount": "0.12"}]), encoding="utf-8")
    incident_path.write_text(
        json.dumps({"incident_key": "i1", "component": "clob api", "severity": "WARNING", "start_ts": "2026-06-25T00:00:00Z"})
        + "\n",
        encoding="utf-8",
    )

    candidates = discover_external_source_files([root])
    report = preview_external_source_import(candidates, base=tmp_path)

    assert report["status"] == "ready"
    assert report["file_count"] == 2
    assert report["records_read"] == 2
    assert report["rows_written"] == 0
    assert report["kind_counts"][REAL_COST_EVENTS] == 1
    assert report["kind_counts"][PLATFORM_INCIDENTS] == 1
    cost_item = next(item for item in report["items"] if item["kind"] == REAL_COST_EVENTS)
    incident_item = next(item for item in report["items"] if item["kind"] == PLATFORM_INCIDENTS)
    assert cost_item["preview"][0]["event_type"] == "GAS_COST"
    assert incident_item["preview"][0]["component"] == "clob_api"


def test_preview_normalizes_external_signal_records(tmp_path: Path) -> None:
    root = tmp_path / "runtime_outputs"
    root.mkdir()
    path = root / "external_signal_events.json"
    path.write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "signal_id": "signal-1",
                        "source": "fixture-signals",
                        "event_type": "external_signal",
                        "market_slug": "fixture-world-cup-winner",
                        "observed_at": "2026-06-25T00:01:00Z",
                        "latency_seconds": "3",
                        "payload": {"signal": "news", "confidence": "0.6"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    candidates = discover_external_source_files([root])
    report = preview_external_source_import(candidates, base=tmp_path)

    assert report["status"] == "ready"
    assert report["kind_counts"][EXTERNAL_SIGNAL_EVENTS] == 1
    item = next(item for item in report["items"] if item["kind"] == EXTERNAL_SIGNAL_EVENTS)
    assert item["missing_required_field_count"] == 0
    assert item["preview"][0]["payload_hash"]


def test_load_external_source_records_supports_wrapped_json(tmp_path: Path) -> None:
    path = tmp_path / "cost-events.json"
    path.write_text(json.dumps({"data": [{"cost_id": "c1", "event_type": "fee", "amount": "0.01"}]}), encoding="utf-8")

    rows = load_external_source_records(path, REAL_COST_EVENTS)

    assert rows == [{"cost_id": "c1", "event_type": "fee", "amount": "0.01"}]


def test_load_external_source_records_supports_order_state_templates(tmp_path: Path) -> None:
    path = tmp_path / "order-state-events.json"
    path.write_text(
        json.dumps({"event_templates": [{"run_id": 42, "order_id": "O-1", "api_order_status": "rejected"}]}),
        encoding="utf-8",
    )

    rows = load_external_source_records(path, REAL_ORDER_STATE_EVENTS)

    assert rows == [{"run_id": 42, "order_id": "O-1", "api_order_status": "rejected"}]


def test_load_external_source_records_supports_external_signal_wrapper(tmp_path: Path) -> None:
    path = tmp_path / "signals.json"
    path.write_text(json.dumps({"external_signals": [{"signal_id": "s1", "observed_at": "2026-06-25T00:00:00Z"}]}), encoding="utf-8")

    rows = load_external_source_records(path, EXTERNAL_SIGNAL_EVENTS)

    assert rows == [{"signal_id": "s1", "observed_at": "2026-06-25T00:00:00Z"}]


def test_discovery_markdown_contains_summary_table(tmp_path: Path) -> None:
    root = tmp_path / "runtime_outputs"
    root.mkdir()
    (root / "wallet_cost_events.jsonl").write_text(
        json.dumps({"cost_id": "c1", "event_type": "rebate", "amount": "0.01"}) + "\n",
        encoding="utf-8",
    )

    report = preview_external_source_import(discover_external_source_files([root]), base=tmp_path)
    markdown = external_source_discovery_to_markdown(report)

    assert "status: ready" in markdown
    assert "wallet_cost_events.jsonl" in markdown
