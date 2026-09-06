from quant.backtest.backtest_engine import ORDERFILLED_CROSS_MODE
from quant.backtest.sample_run import READY, build_fill_first_sample_payload, run_fill_first_sample_backtest


class SequenceCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        if not self.rows:
            return None
        return self.rows.pop(0)


class SequenceConn:
    def __init__(self, rows):
        self.cursor_obj = SequenceCursor(rows)

    def cursor(self):
        return self.cursor_obj


def test_build_sample_payload_clones_latest_fill_first_run() -> None:
    conn = SequenceConn(
        [
            {"run_id": 65},
            {
                "run_id": 65,
                "market_slug": "will-oceania-win-the-2026-fifa-world-cup",
                "token_side": "YES",
                "from_block": 87000000,
                "to_block": 88064180,
                "meta": {"token_id": "token-1"},
                "entry_threshold": "0.001",
                "exit_threshold": "0.0005",
                "initial_capital": "100",
                "position_size": "20",
                "execution_profile": "realistic",
                "order_role": "maker",
                "final_valuation_mode": "SETTLEMENT",
            },
        ]
    )

    candidate, payload = build_fill_first_sample_payload(conn)

    assert candidate.source == "latest_fill_first_run"
    assert candidate.seed_run_id == 65
    assert payload["market_slug"] == "will-oceania-win-the-2026-fifa-world-cup"
    assert payload["token_id"] == "token-1"
    assert payload["from_block"] == 87000000
    assert payload["to_block"] == 88064180
    assert payload["execution_price_mode"] == ORDERFILLED_CROSS_MODE
    assert payload["entry_threshold"] == "0.001"


def test_build_sample_payload_explicit_market_uses_bounded_recent_window() -> None:
    conn = SequenceConn(
        [
            {"from_block": 100, "to_block": 150, "rows": 10},
        ]
    )

    candidate, payload = build_fill_first_sample_payload(
        conn,
        market_slug="demo-market",
        token_side="YES",
        window_rows=10,
    )

    assert candidate.source == "explicit"
    assert candidate.rows == 10
    assert payload["market_slug"] == "demo-market"
    assert payload["from_block"] == 100
    assert payload["to_block"] == 150
    assert "ORDER BY block_number DESC" in conn.cursor_obj.queries[0][0]
    assert conn.cursor_obj.queries[0][1] == ("demo-market", "YES", 10)


def test_sample_backtest_dry_run_returns_payload_without_insert() -> None:
    conn = SequenceConn(
        [
            {"run_id": 65},
            {
                "run_id": 65,
                "market_slug": "seed-market",
                "token_side": "YES",
                "from_block": 1,
                "to_block": 2,
                "meta": {},
                "entry_threshold": None,
                "exit_threshold": None,
                "initial_capital": None,
                "position_size": None,
                "execution_profile": None,
                "order_role": None,
                "final_valuation_mode": None,
            },
        ]
    )

    report = run_fill_first_sample_backtest(conn, dry_run=True)

    assert report["status"] == READY
    assert report["dry_run"] is True
    assert report["run"] is None
    assert report["payload"]["execution_price_mode"] == ORDERFILLED_CROSS_MODE


def test_build_sample_payload_accepts_depth_execution_mode() -> None:
    conn = SequenceConn(
        [
            {"from_block": 100, "to_block": 150, "rows": 10},
        ]
    )

    _, payload = build_fill_first_sample_payload(
        conn,
        market_slug="demo-market",
        token_side="YES",
        window_rows=10,
        execution_price_mode="depth",
    )

    assert payload["execution_price_mode"] == "DEPTH"


def test_build_sample_payload_accepts_fill_smoke_overrides() -> None:
    conn = SequenceConn(
        [
            {"from_block": 100, "to_block": 101, "rows": 2},
        ]
    )

    _, payload = build_fill_first_sample_payload(
        conn,
        market_slug="demo-market",
        order_role="taker",
        buy_limit_price="1",
        sell_limit_price="0",
        settlement_value="1",
        liquidity_cap_pct="100",
        fill_probability_haircut_pct="0",
        position_size="1",
    )

    assert payload["order_role"] == "taker"
    assert payload["buy_limit_price"] == "1"
    assert payload["sell_limit_price"] == "0"
    assert payload["settlement_value"] == "1"
    assert payload["fill_probability_haircut_pct"] == "0"
    assert payload["position_size"] == "1"
