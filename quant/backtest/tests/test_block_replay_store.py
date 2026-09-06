from quant.backtest.runners.block_replay_store import (
    ensure_orderfilled_block_replay_table,
    load_orderfilled_block_replay_rows,
)
from quant.backtest.runners.generic_replay_benchmark import _block_replay_rows_sql
from quant.backtest.runners.generic_replay_benchmark import build_generic_replay_sql_contract_report


class _FakeClickHouse:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.queries: list[str] = []

    def execute(self, sql: str, timeout_seconds: int | None = None) -> None:
        self.executed.append(str(sql))

    def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
        self.queries.append(str(sql))
        return []


def test_block_replay_table_materializes_vwap_and_volume_sides() -> None:
    client = _FakeClickHouse()

    ensure_orderfilled_block_replay_table(client, table="orderfilled_block_replay_test")

    ddl = "\n".join(client.executed)
    assert "vwap_price Decimal(20, 10)" in ddl
    assert "ADD COLUMN IF NOT EXISTS vwap_price" in ddl
    assert "buy_volume Decimal(30, 10)" in ddl
    assert "sell_volume Decimal(30, 10)" in ddl


def test_block_replay_loader_projects_vwap_and_uses_bounded_market_block_range() -> None:
    client = _FakeClickHouse()

    load_orderfilled_block_replay_rows([11, 12], from_block=100, to_block=200, client=client)

    sql = client.queries[-1]
    assert "vwap_price" in sql
    assert "buy_volume" in sql
    assert "sell_volume" in sql
    assert "PREWHERE market_id IN (11,12)" in sql
    assert "block_number BETWEEN 100 AND 200" in sql


def test_generic_replay_benchmark_block_sql_keeps_vwap_and_volume_sides() -> None:
    sql = _block_replay_rows_sql(pairs_sql="(11, 'token-a')", from_block=100, to_block=200)

    assert "AS vwap_price" in sql
    assert "AS buy_volume" in sql
    assert "AS sell_volume" in sql
    assert "PREWHERE (market_id, token_id) IN ((11, 'token-a'))" in sql
    assert "block_number BETWEEN 100 AND 200" in sql


def test_generic_replay_sql_contract_keeps_bounded_raw_and_block_fast_paths() -> None:
    report = build_generic_replay_sql_contract_report()

    assert report["status"] == "ready"
    assert report["missing"] == []
    assert report["checks"]["raw_pair_prewhere"] is True
    assert report["checks"]["raw_pair_limited"] is True
    assert report["checks"]["block_replay_grouped"] is True
    assert report["checks"]["block_replay_vwap"] is True
    assert report["checks"]["block_replay_side_volume"] is True
