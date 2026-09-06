from __future__ import annotations

import hashlib
import json

from quant.backtest.disk_liquidity_ledger import DiskLiquidityLedger


def _hash(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def test_v2_disk_ledger_is_idempotent_and_hash_compatible(tmp_path) -> None:
    ledger = DiskLiquidityLedger(
        tmp_path / "ledger.sqlite3",
        execution_family="V2",
        ledger_id="run:profile",
        source_pin="source",
        profile_hash="profile",
        strategy_hash="strategy",
    )
    first = {
        "source_trade_consumed": {"b": "1.2500000000", "a": "0.5000000000"},
        "market_window_consumed": {},
    }
    second = {
        "source_trade_consumed": {"b": "0.7500000000", "c": "2"},
        "market_window_consumed": {},
    }
    assert ledger.apply_delta(0, "first", first) is True
    assert ledger.apply_delta(0, "first", first) is False
    assert ledger.apply_delta(1, "second", second) is True
    expected = {"a": "0.5000000000", "b": "2.0000000000", "c": "2.0000000000"}
    assert ledger.as_dict() == expected
    assert ledger.canonical_sha256() == _hash(expected)
    ledger.close()


def test_v3_disk_ledger_hash_matches_nested_runtime_shape(tmp_path) -> None:
    ledger = DiskLiquidityLedger(
        tmp_path / "ledger.sqlite3",
        execution_family="V3",
        ledger_id="run:profile",
        source_pin="source",
        profile_hash="profile",
        strategy_hash="strategy",
    )
    delta = {
        "v2": {
            "source_trade_consumed": {"observed": "1"},
            "market_window_consumed": {},
        },
        "inferred_source_capacity": {"inferred": "0.25"},
        "synthetic_arrival_capacity": {"1|asset|BUY|10": "0.5"},
    }
    ledger.apply_delta(0, "delta", delta)
    expected = {
        "ledger_id": "run:profile",
        "v2_source_capacity": {"observed": "1.0000000000"},
        "inferred_source_capacity": {"inferred": "0.2500000000"},
        "synthetic_arrival_capacity": {"1|asset|BUY|10": "0.5000000000"},
    }
    assert ledger.as_dict() == expected
    assert ledger.canonical_sha256() == _hash(expected)
    ledger.close()
