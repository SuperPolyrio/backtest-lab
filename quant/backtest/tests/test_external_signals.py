from quant.backtest.external_signals import build_external_signal_import_report, normalize_external_signal_event


def test_normalize_external_signal_event_generates_payload_hash_and_id() -> None:
    row = {
        "id": "score-1",
        "type": "score_update",
        "timestamp": "2026-06-22T10:00:00Z",
        "blockNumber": 88_000_001,
        "provider": "sports-feed",
        "latency": "1.5",
        "payload": {"home": 1, "away": 0},
        "resolutionSource": "polymarket",
        "settlementRule": "official result",
        "priceToBeatSource": "not_applicable",
        "oracle": "uma",
    }

    event = normalize_external_signal_event(row, run_id=7)

    assert event["run_id"] == 7
    assert event["signal_id"] == "score-1"
    assert event["event_type"] == "score_update"
    assert event["observed_at"] == "2026-06-22T10:00:00Z"
    assert event["observed_block"] == 88_000_001
    assert event["source"] == "sports-feed"
    assert event["latency_seconds"] == "1.5"
    assert len(event["payload_hash"]) == 64
    assert event["resolution_source"] == "polymarket"
    assert event["settlement_rule"] == "official result"
    assert event["price_to_beat_source"] == "not_applicable"
    assert event["oracle_source"] == "uma"


def test_external_signal_import_report_summarizes_missing_contract_fields() -> None:
    report = build_external_signal_import_report([
        {
            "source": "manual",
            "payload": {"note": "missing observed_at and latency"},
        }
    ])

    assert report["status"] == "review"
    assert report["event_count"] == 1
    assert report["missing_required_field_count"] == 2
    assert report["missing_field_counts"] == {"latency_seconds": 1, "observed_at": 1}
