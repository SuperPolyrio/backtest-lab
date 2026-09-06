from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import _price_point, build_data_quality_report, data_quality_metrics
from quant.prices.block_close_algorithm import orderfilled_block_close_sql
from quant.prices.block_close_backfill import insert_block_close_rows
from quant.workers.build_runner import insert_market_block_close_rows


def test_orderfilled_block_close_sql_projects_block_bar_ohlcv_fields() -> None:
    sql = orderfilled_block_close_sql(from_block=100, to_block=200, token_ids=["ABC"])

    assert "f.side_code" in sql
    assert "AS open_price" in sql
    assert "AS high_price" in sql
    assert "AS low_price" in sql
    assert "AS first_tx_hash" in sql
    assert "AS last_tx_hash" in sql
    assert "AS first_log_index" in sql
    assert "AS last_log_index" in sql
    assert "AS buy_volume" in sql
    assert "AS sell_volume" in sql
    assert "AS vwap_price" in sql


class _Copy:
    def __init__(self) -> None:
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def write_row(self, row):
        self.rows.append(row)


class _Cursor:
    def __init__(self) -> None:
        self.queries = []
        self.copy_sql = ""
        self.copy_obj = _Copy()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.queries.append(str(query))

    def copy(self, sql):
        self.copy_sql = str(sql)
        return self.copy_obj


class _Conn:
    def __init__(self) -> None:
        self.cursor_obj = _Cursor()

    def cursor(self):
        return self.cursor_obj


def test_insert_block_close_rows_writes_ohlcv_and_updates_existing_block() -> None:
    conn = _Conn()
    rows_written = insert_block_close_rows(
        conn,
        {
            "abc": {
                "token_id": "token-1",
                "market_id": 11,
                "market_slug": "demo-market",
                "token_side": "YES",
            }
        },
        [
            {
                "token_id": "abc",
                "block_number": 123,
                "block_timestamp": "2026-06-22T12:00:00Z",
                "open_price": "0.20",
                "high_price": "0.25",
                "low_price": "0.18",
                "close_price": "0.22",
                "vwap_price": "0.215",
                "close_raw_price": "0.22",
                "first_tx_hash": "0xfirst",
                "close_tx_hash": "0xlast",
                "last_tx_hash": "0xlast",
                "first_log_index": 3,
                "last_log_index": 9,
                "close_log_index": 9,
                "clean_trade_count": 4,
                "raw_trade_count": 5,
                "volume": "100",
                "buy_volume": "60",
                "sell_volume": "40",
            }
        ],
    )

    copied = conn.cursor_obj.copy_obj.rows[0]
    insert_sql = conn.cursor_obj.queries[-1]

    assert rows_written == 1
    assert "open_price" in conn.cursor_obj.copy_sql
    assert "high_price" in conn.cursor_obj.copy_sql
    assert "low_price" in conn.cursor_obj.copy_sql
    assert "buy_volume" in conn.cursor_obj.copy_sql
    assert "sell_volume" in conn.cursor_obj.copy_sql
    assert "ON CONFLICT (token_id, block_number) DO UPDATE SET" in insert_sql
    assert "open_price = EXCLUDED.open_price" in insert_sql
    assert "buy_volume = EXCLUDED.buy_volume" in insert_sql
    assert copied[6:10] == (Decimal("0.20"), Decimal("0.25"), Decimal("0.18"), Decimal("0.22"))
    assert copied[-2:] == (Decimal("60"), Decimal("40"))


def test_build_runner_block_close_insert_preserves_timestamp_and_ohlcv() -> None:
    conn = _Conn()

    rows_written = insert_market_block_close_rows(
        conn,
        tokens_by_clickhouse_id={
            "abc": {
                "token_id": "token-1",
                "token_id_hex": "abc",
                "market_id": 11,
                "market_slug": "demo-market",
                "token_side": "YES",
            }
        },
        planned_ranges={"abc": (120, 140)},
        rows=[
            {
                "token_id": "abc",
                "block_number": 123,
                "block_timestamp": "2026-06-22T12:00:00Z",
                "open_price": "0.20",
                "high_price": "0.25",
                "low_price": "0.18",
                "close_price": "0.22",
                "vwap_price": "0.215",
                "close_raw_price": "0.22",
                "first_tx_hash": "0xfirst",
                "close_tx_hash": "0xlast",
                "last_tx_hash": "0xlast",
                "first_log_index": 3,
                "last_log_index": 9,
                "close_log_index": 9,
                "clean_trade_count": 4,
                "raw_trade_count": 5,
                "volume": "100",
                "buy_volume": "60",
                "sell_volume": "40",
            }
        ],
    )

    copied = conn.cursor_obj.copy_obj.rows[0]
    insert_sql = conn.cursor_obj.queries[-1]

    assert rows_written == 1
    assert "block_timestamp" in conn.cursor_obj.copy_sql
    assert "open_price" in conn.cursor_obj.copy_sql
    assert "buy_volume" in conn.cursor_obj.copy_sql
    assert "ON CONFLICT (token_id, block_number) DO UPDATE SET" in insert_sql
    assert copied[5] == datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc)
    assert copied[6:10] == (Decimal("0.20"), Decimal("0.25"), Decimal("0.18"), Decimal("0.22"))


def test_price_point_reads_block_bar_fields_and_quality_report_exposes_coverage() -> None:
    point = _price_point(
        {
            "x_value": 123,
            "price": Decimal("0.22"),
            "open_price": Decimal("0.20"),
            "high_price": Decimal("0.25"),
            "low_price": Decimal("0.18"),
            "close_price": Decimal("0.22"),
            "vwap_price": Decimal("0.215"),
            "volume": Decimal("100"),
            "buy_volume": Decimal("60"),
            "sell_volume": Decimal("40"),
            "trade_count": 4,
            "first_log_index": 3,
            "last_log_index": 9,
            "block_timestamp": datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc),
        }
    )

    report = build_data_quality_report(
        [point],
        {
            "price_source": "orderfilled_block_close",
            "market_slug": "demo-market",
            "token_side": "YES",
            "from_block": 123,
            "to_block": 123,
        },
    )
    metrics = data_quality_metrics(report)

    assert point.open_price == Decimal("0.20")
    assert point.high_price == Decimal("0.25")
    assert point.low_price == Decimal("0.18")
    assert point.vwap_price == Decimal("0.215")
    assert point.buy_volume == Decimal("60")
    assert point.sell_volume == Decimal("40")
    assert report["block_bar"]["ohlc_complete_pct"] == "100"
    assert report["block_bar"]["vwap_available_pct"] == "100"
    assert report["block_bar"]["invalid_range_count"] == 0
    assert any(row["metric_key"] == "block_bar_coverage" and row["formatted_value"] == "100%" for row in metrics)


def test_price_point_falls_back_to_close_when_ohlc_is_missing() -> None:
    point = _price_point({"x_value": 1, "price": Decimal("0.42"), "volume": 0, "trade_count": 0})

    assert point.open_price == Decimal("0.42")
    assert point.high_price == Decimal("0.42")
    assert point.low_price == Decimal("0.42")
    assert point.close_price == Decimal("0.42")
