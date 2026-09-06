from types import SimpleNamespace

from quant.backtest.runners.trade_replay_store import (
    backfill_orderfilled_trade_replay,
    build_trade_replay_sql_contract_report,
    ensure_orderfilled_trade_replay_table,
    load_orderfilled_trade_replay_coverage,
    load_orderfilled_trade_replay_rows,
    refresh_orderfilled_trade_replay_coverage,
)


class _FakeClickHouse:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.queries: list[str] = []
        self.scalars: list[str] = []
        self.settings = SimpleNamespace(database="poly_orderfilled", orderfilled_table="orderfilled_fact")

    def execute(self, sql: str, timeout_seconds: int | None = None) -> None:
        self.executed.append(str(sql))

    def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
        self.queries.append(str(sql))
        return []

    def query_scalar(self, sql: str, timeout_seconds: int | None = None):
        self.scalars.append(str(sql))
        return 0


def test_trade_replay_table_materializes_orderfilled_tick_identity() -> None:
    client = _FakeClickHouse()

    ensure_orderfilled_trade_replay_table(client, table="orderfilled_trade_replay_test")

    ddl = "\n".join(client.executed)
    assert "canonical_fill_key String" in ddl
    assert "canonical_fill_key_kind LowCardinality(String)" in ddl
    assert "condition_id String" in ddl
    assert "maker String" in ddl
    assert "taker String" in ddl
    assert "side LowCardinality(String)" in ddl
    assert "block_trade_index UInt32" in ddl
    assert "ORDER BY (market_id, condition_id, token_id, block_number, transaction_index, log_index, tx_hash, canonical_fill_key)" in ddl


def test_trade_replay_loader_uses_bounded_token_pair_tick_order() -> None:
    client = _FakeClickHouse()

    load_orderfilled_trade_replay_rows(
        [(12, "TOKEN-B"), (11, "token-a")],
        from_block=100,
        to_block=200,
        client=client,
        table="orderfilled_trade_replay_test",
    )

    sql = client.queries[-1]
    assert "PREWHERE (market_id, token_id) IN ((11, 'token-a'),(12, 'token-b'))" in sql
    assert "block_number BETWEEN 100 AND 200" in sql
    assert "ORDER BY market_id ASC, condition_id ASC, token_id ASC, block_number ASC, transaction_index ASC, log_index ASC, tx_hash ASC, canonical_fill_key ASC" in sql
    assert "LIMIT 10000000" in sql
    assert "canonical_fill_key" in sql
    assert "canonical_fill_key_kind" in sql
    assert "condition_id" in sql
    assert "side AS side_code" in sql


def test_trade_replay_backfill_uses_configured_orderfilled_source_table() -> None:
    client = _FakeClickHouse()
    client.settings = SimpleNamespace(database="poly_orderfilled", orderfilled_table="orderfilled_fact_sample")

    backfill_orderfilled_trade_replay(
        [(12, "TOKEN-B")],
        from_block=100,
        to_block=200,
        client=client,
        build_tag="unit_test",
    )

    sql = "\n".join(client.executed)
    assert "FROM orderfilled_fact_sample" in sql
    assert "'orderfilled_fact_sample' AS source_table" in sql
    assert "PREWHERE (market_id, token_id) IN ((12, 'token-b'))" in sql
    assert "block_number BETWEEN 100 AND 200" in sql


def test_trade_replay_coverage_loader_uses_latest_covering_row_not_duplicate_sum() -> None:
    client = _FakeClickHouse()

    load_orderfilled_trade_replay_coverage(
        [(12, "TOKEN-B")],
        from_block=100,
        to_block=200,
        client=client,
        coverage_table="orderfilled_trade_replay_coverage_test",
    )

    sql = client.queries[-1]
    assert "argMax(row_count, updated_at) AS row_count" in sql
    assert "argMax(canonical_event_count, updated_at) AS canonical_event_count" in sql
    assert "argMax(data_version, updated_at) AS loaded_data_version" in sql
    assert "max(updated_at) AS latest_updated_at" in sql
    assert "FROM (" in sql
    assert "sum(row_count)" not in sql
    assert "from_block <= 100" in sql
    assert "to_block >= 200" in sql


def test_trade_replay_coverage_refresh_matches_coverage_table_shape() -> None:
    client = _FakeClickHouse()

    refresh_orderfilled_trade_replay_coverage(
        [(12, "TOKEN-B")],
        from_block=100,
        to_block=200,
        client=client,
        replay_table="orderfilled_trade_replay_test",
        coverage_table="orderfilled_trade_replay_coverage_test",
    )

    insert_sql = next(sql for sql in client.executed if "INSERT INTO orderfilled_trade_replay_coverage_test" in sql)
    select_head = insert_sql.split("FROM orderfilled_trade_replay_test", 1)[0]
    assert "condition_id" not in select_head
    assert "market_id" in select_head
    assert "token_id" in select_head
    assert "GROUP BY market_id, token_id" in insert_sql


def test_trade_replay_sql_contract_requires_canonical_attribution_and_ordering() -> None:
    report = build_trade_replay_sql_contract_report()

    assert report["status"] == "ready"
    assert report["missing"] == []
    assert report["checks"]["canonical_fill_key_persisted"] is True
    assert report["checks"]["condition_id_persisted"] is True
    assert report["checks"]["maker_taker_side_persisted"] is True
    assert report["checks"]["block_trade_index_persisted"] is True
    assert report["checks"]["loader_pair_prewhere"] is True
    assert report["checks"]["loader_tick_ordered"] is True
