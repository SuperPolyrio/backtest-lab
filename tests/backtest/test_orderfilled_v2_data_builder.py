from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from quant.core import db as db_module
from quant.backtest import fill_trade_validation as contract_validation
from scripts import build_orderfilled_v2_data as subject


def test_builder_identity_hashes_the_exact_running_script() -> None:
    identity = subject.builder_identity()

    assert identity["canonical_identity_contract"] == "ORDERFILLED_SEVEN_FIELD_V1"
    assert identity["script_path"] == str(Path(subject.__file__).resolve())
    assert identity["script_sha256"] == hashlib.sha256(
        Path(subject.__file__).read_bytes()
    ).hexdigest()


class _Client:
    def __init__(self, *, missing: int = 0) -> None:
        self.missing = missing
        self.queries: list[str] = []

    def query_json_rows(self, query: str, *, timeout_seconds: int | None = None):
        self.queries.append(query)
        return [
            {
                "source_blocks": 10,
                "trusted_timestamp_blocks": 10 - self.missing,
                "missing_timestamp_blocks": self.missing,
                "invalid_timestamp_blocks": 0,
                "missing_hash_blocks": 0,
            }
        ]

    def query_scalar(self, query: str, *, timeout_seconds: int | None = None):
        self.queries.append(query)
        return "0"

    def execute(
        self,
        query: str,
        *,
        stdin: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        self.queries.append(query)


def test_block_time_preflight_fails_when_any_source_block_lacks_rpc_header() -> None:
    client = _Client(missing=1)

    result = subject.validate_trusted_block_time_coverage(client, 100, 200)

    assert result["status"] == "incomplete"
    assert result["missing_timestamp_blocks"] == 1
    assert all("trade_time" not in query for query in client.queries)


def test_source_cte_uses_deduplicated_rpc_headers_without_time_extrapolation() -> None:
    query = subject.source_with_time_cte(100, 200)

    assert "extrapolated_from_last_block_timestamp" not in query
    assert "addSeconds" not in query
    assert "WITH canonical_source AS" in query
    assert "argMax(" in query
    assert "GROUP BY lower(tx_hash), log_index, market_id" in query
    assert "INNER JOIN" in query
    assert "GROUP BY block_number" in query
    assert "startsWith(lower(source), 'rpc')" in query
    assert "concat('block_timestamps:', bt.trusted_source)" in query


def test_market_asset_map_incrementally_inserts_only_unseen_assets(monkeypatch) -> None:
    client = _Client()
    counts = iter((3, 5))
    monkeypatch.setattr(subject, "count_rows", lambda *args, **kwargs: next(counts))

    result = subject.insert_market_asset_map(
        client,
        100,
        200,
        chain_id=137,
    )

    insert = next(
        query
        for query in client.queries
        if "INSERT INTO market_asset_map" in query
    )
    assert "lower(token_id) NOT IN" in insert
    assert "WHERE chain_id = toUInt64(137)" in insert
    assert result["inserted_estimate"] == 2


class _IdentityClient:
    def __init__(
        self, *, payload_conflicts: int = 0, tx_log_conflicts: int = 0
    ) -> None:
        self.payload_conflicts = payload_conflicts
        self.tx_log_conflicts = tx_log_conflicts
        self.queries: list[str] = []

    def query_json_rows(self, query: str, *, timeout_seconds: int | None = None):
        self.queries.append(query)
        return [
            {
                "physical_source_rows": 12,
                "canonical_source_rows": 10,
                "duplicate_extra_rows": 2,
                "immutable_payload_conflict_identities": self.payload_conflicts,
            }
        ]

    def query_scalar(self, query: str, *, timeout_seconds: int | None = None):
        self.queries.append(query)
        return str(self.tx_log_conflicts)


def test_source_identity_preflight_allows_exact_physical_duplicates() -> None:
    client = _IdentityClient()

    result = subject.validate_source_identity_quality(client, 100, 200)

    assert result == {
        "physical_source_rows": 12,
        "canonical_source_rows": 10,
        "duplicate_extra_rows": 2,
        "immutable_payload_conflict_identities": 0,
        "tx_log_identity_conflicts": 0,
        "status": "ready",
    }
    assert "uniqExact(tuple(" in client.queries[0]
    assert "GROUP BY lower(tx_hash), log_index, market_id" in client.queries[0]


def test_source_identity_preflight_rejects_immutable_payload_conflicts() -> None:
    result = subject.validate_source_identity_quality(
        _IdentityClient(payload_conflicts=1), 100, 200
    )

    assert result["status"] == "conflict"


def test_receipts_are_published_only_after_full_validation(monkeypatch) -> None:
    calls: list[str] = []

    def fake_record_chunk(client, **kwargs) -> None:
        calls.append(kwargs["table_name"])

    monkeypatch.setattr(subject, "record_chunk", fake_record_chunk)
    monkeypatch.setattr(subject, "chunk_record_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        subject, "delete_all_chunk_records", lambda *args, **kwargs: None
    )
    result = subject.ChunkResult(
        start_block=100,
        end_block=200,
        table_results={
            table: {
                "status": "inserted",
                "after_rows": index + 1,
                "elapsed_sec": 0.1,
            }
            for index, table in enumerate(subject.DEFAULT_TABLES)
        },
        elapsed_sec=1.0,
    )

    blocked = subject.publish_validated_chunk_records(
        object(),
        result=result,
        validation={"status": "incomplete", "counts": {}},
        build_tag="test",
    )
    assert blocked == []
    assert calls == []

    published = subject.publish_validated_chunk_records(
        object(),
        result=result,
        validation={
            "status": "ready",
            "counts": {"source_orderfilled_fact_canonical": 10},
        },
        build_tag="test",
    )
    assert published == [
        "raw_orderfilled",
        "maker_fill_ticks",
        "orderfilled_quarantine",
        "block_trade_bars_sparse",
        "trade_prints_one_sided",
    ]
    assert calls[-1] == "trade_prints_one_sided"


def test_recovered_chunk_publishes_missing_receipts_without_rebuild(
    monkeypatch,
) -> None:
    recorded: list[str] = []

    monkeypatch.setattr(subject, "chunk_record_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        subject, "delete_all_chunk_records", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        subject,
        "record_chunk",
        lambda client, **kwargs: recorded.append(kwargs["table_name"]),
    )
    result = subject.ChunkResult(
        start_block=100,
        end_block=200,
        table_results={
            table: {
                "status": "recovered_existing",
                "after_rows": index,
                "elapsed_sec": 0.0,
            }
            for index, table in enumerate(subject.DEFAULT_TABLES)
        },
        elapsed_sec=0.0,
    )

    published = subject.publish_validated_chunk_records(
        object(),
        result=result,
        validation={
            "status": "ready",
            "counts": {"source_orderfilled_fact_canonical": 10},
        },
        build_tag="test",
    )

    assert published == [
        "raw_orderfilled",
        "maker_fill_ticks",
        "orderfilled_quarantine",
        "block_trade_bars_sparse",
        "trade_prints_one_sided",
    ]
    assert recorded == published


def test_recovered_chunk_does_not_duplicate_existing_receipts(monkeypatch) -> None:
    monkeypatch.setattr(subject, "chunk_record_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        subject,
        "record_chunk",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unexpected write")
        ),
    )
    result = subject.ChunkResult(
        start_block=100,
        end_block=200,
        table_results={
            table: {"status": "recovered_existing", "after_rows": 1}
            for table in subject.DEFAULT_TABLES
        },
        elapsed_sec=0.0,
    )

    assert (
        subject.publish_validated_chunk_records(
            object(),
            result=result,
            validation={
                "status": "ready",
                "counts": {"source_orderfilled_fact_canonical": 10},
            },
            build_tag="test",
        )
        == []
    )


def test_recover_existing_chunk_requires_full_validation(monkeypatch) -> None:
    monkeypatch.setattr(subject, "count_rows", lambda *args, **kwargs: 5)
    monkeypatch.setattr(
        subject,
        "validate_layers",
        lambda *args, **kwargs: {"status": "incomplete"},
    )

    assert (
        subject.recover_validated_existing_chunk(
            object(),
            start_block=100,
            end_block=200,
            tables=subject.DEFAULT_TABLES,
            chain_id=137,
        )
        is None
    )


def test_conservation_tolerance_is_bounded() -> None:
    assert subject._conserved("10", "10") is True
    assert subject._conserved("10.0000000001", "10") is False
    assert subject._conserved("10.0004", "10") is False


def test_receipt_revocation_covers_overlapping_intervals(monkeypatch) -> None:
    queries: list[str] = []

    class Client:
        def execute(self, query: str, *, timeout_seconds: int | None = None) -> None:
            queries.append(query)

    monkeypatch.setattr(subject, "table_exists", lambda *args, **kwargs: True)

    subject.delete_all_chunk_records(Client(), "trade_prints_one_sided", 100, 200)

    assert "to_block < toUInt64(100)" in queries[0]
    assert "from_block > toUInt64(200)" in queries[0]
    assert "from_block = toUInt64(100)" not in queries[0]


def test_external_contract_fails_when_source_is_empty_but_derived_rows_exist() -> None:
    class Client:
        def query_scalar(self, query: str, *, timeout_seconds: int | None = None):
            return "1"

        def query_json_rows(self, query: str, *, timeout_seconds: int | None = None):
            if "AS physical_rows" in query:
                return [{"physical_rows": 0, "canonical_rows": 0}]
            if "GROUP BY passive_side, aggressor_side" in query:
                return [{"passive_side": "BUY", "aggressor_side": "SELL", "rows": 1}]
            if "FROM raw_orderfilled" in query:
                return [
                    {
                        "invalid_price_count": 0,
                        "invalid_size_count": 0,
                        "missing_key_count": 0,
                        "duplicate_tx_log_orderhash_count": 0,
                        "missing_timestamp_count": 0,
                    }
                ]
            if "FROM maker_fill_ticks" in query:
                return [
                    {
                        "invalid_price_count": 0,
                        "invalid_size_count": 0,
                        "missing_mapping_count": 0,
                        "invalid_side_count": 0,
                    }
                ]
            if "FROM trade_prints_one_sided" in query:
                return [
                    {
                        "invalid_price_count": 0,
                        "invalid_size_count": 0,
                        "missing_mapping_count": 0,
                        "invalid_side_count": 0,
                        "missing_source_fill_count": 0,
                        "source_fill_count_sum": 1,
                        "duplicate_trade_id_count": 0,
                    }
                ]
            return []

    result = contract_validation.validate_orderfilled_data_contract(
        client=Client(), from_block=100, to_block=200
    )

    assert result["status"] == "fail"
    canonical_check = next(
        row
        for row in result["checks"]
        if row["name"] == "raw matches canonical source orderfilled_fact"
    )
    assert canonical_check["status"] == "FAIL"


def test_clickhouse_subprocess_failure_redacts_password(monkeypatch) -> None:
    secret = "do-not-log-this"
    settings = db_module.ClickHouseSettings(
        http_url="",
        container="clickhouse",
        database="poly_orderfilled",
        user="poly_user",
        password=secret,
        orderfilled_table="orderfilled_fact",
        timeout_seconds=1,
    )
    client = db_module.ClickHouseClient(settings)
    monkeypatch.setattr(db_module.shutil, "which", lambda _: "/usr/bin/docker")

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(
            85,
            ["docker", "--password", secret],
            stderr=f"server rejected password {secret}",
        )

    monkeypatch.setattr(db_module.subprocess, "run", fail)

    with pytest.raises(RuntimeError) as exc_info:
        client.query_scalar("SELECT 1")

    assert secret not in str(exc_info.value)
    assert "[REDACTED]" in str(exc_info.value)
    assert "exit code 85" in str(exc_info.value)


def test_clickhouse_docker_transport_keeps_secret_and_query_out_of_argv(
    monkeypatch,
) -> None:
    secret = "not-visible-in-process-list"
    settings = db_module.ClickHouseSettings(
        http_url="",
        container="clickhouse",
        database="poly_orderfilled",
        user="poly_user",
        password=secret,
        orderfilled_table="orderfilled_fact",
        timeout_seconds=1,
    )
    client = db_module.ClickHouseClient(settings)
    captured = {}
    monkeypatch.setattr(db_module.shutil, "which", lambda _: "/usr/bin/docker")

    def succeed(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="1\n", stderr="")

    monkeypatch.setattr(db_module.subprocess, "run", succeed)

    assert client.query_scalar("SELECT 1") == "1"
    assert secret not in captured["command"]
    assert "--password" not in captured["command"]
    assert "--query" not in captured["command"]
    assert captured["command"][:6] == [
        "docker",
        "exec",
        "-i",
        "--env",
        "CLICKHOUSE_PASSWORD",
        "clickhouse",
    ]
    assert captured["input"] == "SELECT 1\n"
    assert captured["env"]["CLICKHOUSE_PASSWORD"] == secret
