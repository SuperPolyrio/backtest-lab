from decimal import Decimal
import subprocess
import sys
from pathlib import Path

from quant.backtest.backtest_engine import replace_backtest_results
from quant.backtest.fill_evidence import (
    build_fill_evidence_validation_report,
    fill_evidence_validation_report_to_markdown,
)
from quant.backtest import run_artifacts
from quant.backtest.orders import enrich_order_evidence_fields, order_from_fill
from quant.core import schema


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_fill_evidence_validation_report_summarizes_raw_and_block_bar_orders() -> None:
    orders = [
        {
            "order_id": "o1",
            "status": "FILLED",
            "filled_size": Decimal("10"),
            "execution_source": "orderfilled_limit_replay_raw",
            "meta": {
                "consumed_events": [{"tx_hash": "0x1", "log_index": 1, "size": "10", "trade_price": "0.49"}],
                "candidate_events": [{"tx_hash": "0x1", "log_index": 1, "size": "10", "trade_price": "0.49"}],
                "fill_schedule": [
                    {
                        "market_id": 42,
                        "token_id": "token-yes",
                        "block_number": 101,
                        "fillable_size": Decimal("2.5"),
                        "side_compatibility": "compatible",
                        "side_compatibility_factor": Decimal("1"),
                        "block_buy_volume": Decimal("4"),
                        "block_sell_volume": Decimal("6"),
                        "block_unknown_side_volume": Decimal("0"),
                        "block_buy_notional": Decimal("1.96"),
                        "block_sell_notional": Decimal("2.94"),
                        "block_unknown_side_notional": Decimal("0"),
                        "block_vwap_price": Decimal("0.49"),
                    },
                    {
                        "market_id": 42,
                        "token_id": "token-yes",
                        "block_number": 101,
                        "fillable_size": Decimal("1.5"),
                        "side_compatibility": "incompatible_discounted",
                        "side_compatibility_factor": Decimal("0.4"),
                        "block_buy_volume": Decimal("4"),
                        "block_sell_volume": Decimal("6"),
                        "block_unknown_side_volume": Decimal("0"),
                        "block_buy_notional": Decimal("1.96"),
                        "block_sell_notional": Decimal("2.94"),
                        "block_unknown_side_notional": Decimal("0"),
                        "block_vwap_price": Decimal("0.49"),
                    },
                ],
            },
        },
        {
            "order_id": "o2",
            "status": "FILLED",
            "filled_size": Decimal("5"),
            "execution_source": "orderfilled_limit_replay_synthetic",
            "meta": {
                "block_bar_used_for_fill": True,
                "block_bar_cross_field": "low_price",
                "block_bar_cross_price": Decimal("0.49"),
            },
        },
        {
            "order_id": "o3",
            "status": "NO_FILL",
            "filled_size": Decimal("0"),
            "execution_source": "orderfilled_limit_replay",
            "no_fill_reason": "buy_limit_not_crossed",
            "meta": {},
        },
    ]

    report = build_fill_evidence_validation_report(orders)

    assert report["status"] == "review"
    assert report["submitted_count"] == 3
    assert report["filled_count"] == 2
    assert report["raw_orderfilled_fill_count"] == 1
    assert report["block_bar_synthetic_fill_count"] == 1
    assert report["raw_replay_coverage_pct"] == "50"
    assert report["block_bar_fallback_pct"] == "50"
    assert report["orders"][0]["execution_evidence_type"] == "raw_orderfilled"
    assert report["orders"][0]["fill_schedule_tick_count"] == 2
    assert report["orders"][0]["fill_schedule_fillable_size"] == "4"
    assert report["orders"][0]["side_compatibility_counts"] == {"compatible": 1, "incompatible_discounted": 1}
    assert report["orders"][0]["side_discounted_tick_count"] == 1
    assert report["orders"][0]["block_side_context_count"] == 1
    assert report["orders"][0]["block_buy_volume"] == "4"
    assert report["orders"][0]["block_sell_volume"] == "6"
    assert report["orders"][0]["block_side_contexts"][0]["buy_notional"] == "1.96"
    assert report["orders"][1]["execution_evidence_type"] == "block_bar_ohlcv_fallback"
    assert report["orders"][2]["execution_evidence_type"] == "none"


def test_fill_evidence_markdown_includes_order_rows() -> None:
    report = {
        "status": "ready",
        "reason": "ok",
        "submitted_count": 1,
        "filled_count": 1,
        "no_fill_count": 0,
        "raw_orderfilled_fill_count": 1,
        "block_bar_synthetic_fill_count": 0,
        "raw_replay_coverage_pct": "100",
        "block_bar_fallback_pct": "0",
        "orders": [
            {
                "order_id": "o1",
                "status": "FILLED",
                "execution_evidence_type": "raw_orderfilled",
                "raw_candidate_event_count": 1,
                "raw_consumed_event_count": 1,
                "fill_schedule_tick_count": 2,
                "fill_schedule_fillable_size": "4",
                "side_discounted_tick_count": 1,
                "block_buy_volume": "4",
                "block_sell_volume": "6",
                "block_unknown_side_volume": "0",
            }
        ],
    }

    markdown = fill_evidence_validation_report_to_markdown(report)

    assert "# Fill Evidence Validation: ready" in markdown
    assert "raw_replay_coverage_pct: 100%" in markdown
    assert "| o1 | FILLED | raw_orderfilled | 1 | 1 | 2 | 4 | 1 | 4/6/0 |" in markdown


def test_order_schema_persists_evidence_columns() -> None:
    table_sql = "\n".join(schema.CREATE_TABLE_SQL)
    migrations = "\n".join(schema.ALTER_TABLE_SQL)

    for field in (
        "execution_evidence_type",
        "raw_candidate_event_count",
        "raw_consumed_event_count",
        "block_bar_crossed",
        "block_bar_cross_field",
        "block_bar_cross_price",
    ):
        assert field in table_sql
        assert field in migrations


def test_read_api_enriches_legacy_order_evidence_from_meta() -> None:
    row = {
        "order_id": "o1",
        "status": "FILLED",
        "filled_size": Decimal("10"),
        "execution_source": "orderfilled_limit_replay_synthetic",
        "execution_evidence_type": "unknown",
        "raw_candidate_event_count": 0,
        "raw_consumed_event_count": 0,
        "block_bar_crossed": False,
        "block_bar_cross_field": None,
        "block_bar_cross_price": None,
        "meta": {
            "block_bar_used_for_fill": True,
            "block_bar_crossed": True,
            "block_bar_cross_field": "low_price",
            "block_bar_cross_price": "0.49",
            "candidate_events": [{"tx_hash": "synthetic"}],
            "consumed_events": [{"tx_hash": "synthetic"}],
        },
    }

    enriched = enrich_order_evidence_fields(row)

    assert enriched["execution_evidence_type"] == "block_bar_ohlcv_fallback"
    assert enriched["raw_candidate_event_count"] == 1
    assert enriched["raw_consumed_event_count"] == 1
    assert enriched["block_bar_crossed"] is True
    assert enriched["block_bar_cross_field"] == "low_price"
    assert enriched["block_bar_cross_price"] == "0.49"


def test_modeled_expected_fill_keeps_zero_actual_execution() -> None:
    order = order_from_fill(
        order_id="modeled-1",
        signal_index=1,
        x_axis="block_number",
        x_value=100,
        side="BUY_YES",
        role="taker",
        order_type="FAK",
        decision_price=Decimal("0.50"),
        fill={
            "fill_status": "MODELED_EXPECTATION",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("3"),
            "expected_fill_size": Decimal("3"),
            "actual_fill_size": Decimal("0"),
            "filled_notional": Decimal("1.5"),
            "expected_fill_notional": Decimal("1.5"),
            "actual_fill_notional": Decimal("0"),
            "avg_fill_price": Decimal("0.50"),
            "execution_evidence_type": "modeled_orderfilled_expectation",
        },
    )

    assert order["status"] == "MODELED_EXPECTATION"
    assert order["expected_fill_size"] == Decimal("3")
    assert order["actual_fill_size"] == Decimal("0")
    assert order["actual_fill_notional"] == Decimal("0")
    assert order["execution_evidence_type"] == "modeled_orderfilled_expectation"


def test_replace_backtest_results_inserts_order_evidence_columns() -> None:
    conn = FakeConn()
    result = {
        "metrics": [],
        "equity": [],
        "trades": [],
        "orders": [
            {
                "order_id": "o1",
                "signal_index": 1,
                "trade_id": "t1",
                "x_axis": "block_number",
                "signal_x": 100,
                "submit_x": 100,
                "decision_price": Decimal("0.50"),
                "requested_price": Decimal("0.49"),
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "post_only_limit",
                "status": "FILLED",
                "requested_size": Decimal("10"),
                "requested_notional": Decimal("5"),
                "expected_fill_size": Decimal("10"),
                "expected_fill_notional": Decimal("4.9"),
                "actual_fill_size": Decimal("10"),
                "actual_fill_notional": Decimal("4.9"),
                "filled_size": Decimal("10"),
                "filled_notional": Decimal("4.9"),
                "unfilled_size": Decimal("0"),
                "avg_fill_price": Decimal("0.49"),
                "fill_probability": Decimal("100"),
                "fill_pct": Decimal("100"),
                "block_volume": Decimal("10"),
                "trade_count": 1,
                "available_notional": Decimal("4.9"),
                "participation_rate": Decimal("100"),
                "fee_cost": Decimal("0"),
                "rebate": Decimal("0"),
                "slippage_cost": Decimal("0"),
                "execution_cost": Decimal("0"),
                "latency_blocks": 0,
                "latency_seconds": Decimal("0"),
                "no_fill_reason": None,
                "execution_source": "orderfilled_limit_replay_synthetic",
                "execution_evidence_type": "block_bar_ohlcv_fallback",
                "raw_candidate_event_count": 1,
                "raw_consumed_event_count": 1,
                "block_bar_crossed": True,
                "block_bar_cross_field": "low_price",
                "block_bar_cross_price": Decimal("0.49"),
                "meta": {"execution_evidence_type": "block_bar_ohlcv_fallback"},
            }
        ],
        "ledger": [],
        "events": [],
    }

    replace_backtest_results(conn, 7, result)

    order_insert = next(call for call in conn.cursor_obj.executemany_calls if "quant.quant_backtest_orders" in call[0])
    assert "execution_evidence_type" in order_insert[0]
    assert "raw_candidate_event_count" in order_insert[0]
    assert "block_bar_cross_price" in order_insert[0]
    values = order_insert[1][0]
    assert order_insert[0].count("%s") == len(values)
    assert "block_bar_ohlcv_fallback" in values
    assert "low_price" in values


def test_repair_backtest_run_fill_evidence_artifacts_updates_order_columns(monkeypatch) -> None:
    conn = FakeConn()
    inputs = {
        "run": {"run_id": 7, "meta": {"actual_data_quality": {"status": "ready"}}},
        "orders": [
            {
                "order_id": "o1",
                "status": "FILLED",
                "filled_size": Decimal("10"),
                "execution_source": "orderfilled_limit_replay_synthetic",
                "execution_evidence_type": "unknown",
                "raw_candidate_event_count": 0,
                "raw_consumed_event_count": 0,
                "block_bar_crossed": False,
                "block_bar_cross_field": None,
                "block_bar_cross_price": None,
                "meta": {
                    "block_bar_used_for_fill": True,
                    "block_bar_crossed": True,
                    "block_bar_cross_field": "low_price",
                    "block_bar_cross_price": "0.49",
                    "candidate_events": [{"tx_hash": "synthetic"}],
                    "consumed_events": [{"tx_hash": "synthetic"}],
                },
            }
        ],
    }
    monkeypatch.setattr(run_artifacts, "load_backtest_run_artifact_inputs", lambda _conn, *, run_id: inputs)
    monkeypatch.setattr(run_artifacts, "build_backtest_run_artifact_report", lambda _inputs, *, run_id=None: {"status": "ready", "run_id": run_id})
    monkeypatch.setattr(
        run_artifacts,
        "repair_backtest_run_fill_quality_artifacts",
        lambda _conn, *, run_id, force=False: {"status": "ready", "run_id": run_id, "updated": True},
    )

    report = run_artifacts.repair_backtest_run_fill_evidence_artifacts(conn, run_id=7)

    assert report["updated"] is True
    assert report["orders_updated"] == 1
    assert report["execution_evidence_counts"] == {"block_bar_ohlcv_fallback": 1}
    update_call = next(call for call in conn.cursor_obj.executemany_calls if "UPDATE quant.quant_backtest_orders" in call[0])
    assert "execution_evidence_type" in update_call[0]
    assert update_call[1][0][0] == "block_bar_ohlcv_fallback"
    assert update_call[1][0][1] == 1
    assert update_call[1][0][2] == 1
    assert update_call[1][0][3] is True
    assert update_call[1][0][4] == "low_price"


def test_audit_backtest_run_artifacts_help_includes_fill_evidence_repair() -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "audit_backtest_run_artifacts.py"), "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "--repair-fill-evidence" in result.stdout
    assert "--no-rebuild-fill-quality" in result.stdout


class FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def executemany(self, sql: str, values: list[tuple]) -> None:
        self.executemany_calls.append((sql, values))


class FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_obj
