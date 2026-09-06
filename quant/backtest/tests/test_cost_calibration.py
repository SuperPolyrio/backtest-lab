from quant.backtest.cost_calibration import (
    build_cost_calibration_report,
    build_cost_calibration_samples,
    empty_cost_calibration_report,
    normalize_real_cost_event,
)


def test_normalize_real_cost_event_accepts_wallet_shapes() -> None:
    row = normalize_real_cost_event(
        {
            "id": "wallet-1",
            "runId": 7,
            "costType": "gas",
            "orderId": "entry-1",
            "amount": "-0.12",
            "asset": "USDC",
            "txHash": "0xabc",
        },
        source="wallet-ledger",
    )

    assert row["source"] == "wallet-ledger"
    assert row["run_id"] == 7
    assert row["event_type"] == "GAS_COST"
    assert row["amount"].to_eng_string() == "0.1200000000"
    assert row["cost_id"] == "wallet-1"


def test_build_cost_calibration_samples_matches_fee_rebate_and_external_costs() -> None:
    ledger_rows = [
        {
            "run_id": 7,
            "ledger_id": "buy-1",
            "event_type": "BUY",
            "order_id": "entry-1",
            "trade_id": "trade-1",
            "market_slug": "demo",
            "token_side": "YES",
            "fee": "0.02",
            "rebate": "0.005",
        },
        {
            "run_id": 7,
            "ledger_id": "gas-1",
            "event_type": "GAS_COST",
            "order_id": "entry-1",
            "trade_id": "trade-1",
            "market_slug": "demo",
            "token_side": "YES",
            "cash_delta": "-0.10",
        },
    ]
    real_events = [
        {"run_id": 7, "cost_id": "fee-live", "event_type": "fee", "order_id": "entry-1", "trade_id": "trade-1", "amount": "0.03"},
        {"run_id": 7, "cost_id": "rebate-live", "event_type": "rebate", "order_id": "entry-1", "trade_id": "trade-1", "amount": "0.005"},
        {"run_id": 7, "cost_id": "gas-live", "event_type": "GAS_COST", "order_id": "entry-1", "trade_id": "trade-1", "amount": "0.10"},
    ]

    samples = build_cost_calibration_samples(ledger_rows, real_events, source="wallet-ledger", run_id=7)
    by_type = {sample["event_type"]: sample for sample in samples}

    assert by_type["FEE"]["verdict"] == "amount_mismatch"
    assert by_type["FEE"]["amount_error"].to_eng_string() == "0.0100000000"
    assert by_type["REBATE"]["verdict"] == "matched"
    assert by_type["GAS_COST"]["verdict"] == "matched"


def test_build_cost_calibration_report_flags_missing_live_costs() -> None:
    samples = build_cost_calibration_samples(
        [{"ledger_id": "settle-cost", "event_type": "SETTLEMENT_COST", "trade_id": "t1", "cash_delta": "-0.25"}],
        [],
        source="wallet-ledger",
    )
    report = build_cost_calibration_report(samples)

    assert report["sample_count"] == 1
    assert report["missing_live_count"] == 1
    assert report["requires_recalibration"] is True
    assert report["event_type_summary"]["SETTLEMENT_COST"]["amount_error"] == "0.25"


def test_empty_cost_calibration_report_is_frontend_stable() -> None:
    report = empty_cost_calibration_report("table missing")

    assert report["sample_count"] == 0
    assert report["total_amount_error"] == "0"
    assert report["event_type_summary"] == {}
    assert report["trust_status"] == "unknown"
    assert report["trust_reason"] == "table missing"
