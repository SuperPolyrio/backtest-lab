from __future__ import annotations

import gzip
import json
from pathlib import Path

from quant.backtest.fill_only_financials import load_fill_only_execution_positions
from quant.backtest.market_settlement import file_sha256
from quant.backtest.timestamp_native_position_adapter import (
    _canonical_sha256,
    _condensed_positive_result,
    _resolve_token_candidates,
    export_timestamp_native_execution_positions,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def test_completed_timestamp_native_profile_exports_generic_position(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "execution"
    day = "2026-02-01"
    profile = "central"
    day_root = root / "days" / day
    result_path = day_root / "profiles" / profile / "results.jsonl.gz"
    result_path.parent.mkdir(parents=True)
    asset_id = "a" * 64
    order_id = f"strategy:{day}T00:00:00+00:00:{asset_id}:buy"
    row = {
        "schema_version": "PMQ073TimestampNativeMatcherResultV2",
        "profile": profile,
        "signal_day": day,
        "global_order_ordinal": 0,
        "frozen_order_sha256": "f" * 64,
        "matcher_result": {
            "order_id": order_id,
            "side": "BUY",
            "status": "FILLED",
            "filled_size": "10.0000000000",
            "filled_notional": "5.1000000000",
            "fills": [
                {
                    "filled_size": "10.0000000000",
                    "exec_price": "0.5100000000",
                    "fill_block": 900,
                    "fill_ts": "2026-02-01T00:00:02+00:00",
                    "source_trade_id": "source-1",
                }
            ],
        },
    }
    with gzip.open(result_path, "wt", encoding="utf-8") as stream:
        stream.write(json.dumps(row, separators=(",", ":")) + "\n")

    execution_sha = "e" * 64
    checkpoint = {
        "schema_version": "PMQ073TimestampNativeDayCheckpointV3",
        "execution_manifest_sha256": execution_sha,
        "signal_day": day,
        "previous_day_checkpoint_sha256": "0" * 64,
        "profile_results": {
            profile: {
                "file": f"profiles/{profile}/results.jsonl.gz",
                "file_sha256": file_sha256(result_path),
                "result_count": 1,
            }
        },
    }
    checkpoint["day_checkpoint_sha256"] = _canonical_sha256(checkpoint)
    _write_json(day_root / "checkpoint.json", checkpoint)
    manifest = {"execution_manifest_sha256": execution_sha}
    _write_json(root / "execution_manifest.json", manifest)
    state = {
        "schema_version": "PMQ073TimestampNativeExecutionStateV3",
        "status": "COMPLETE",
        "execution_manifest_sha256": execution_sha,
        "completed_days": [day],
        "last_day_checkpoint_sha256": checkpoint["day_checkpoint_sha256"],
        "selected_day_count": 1,
    }
    state["state_sha256"] = _canonical_sha256(state)
    _write_json(root / "execution_state.json", state)
    _write_json(
        root / "profile_summaries.json",
        {
            "execution_manifest_sha256": execution_sha,
            "profiles": {
                profile: {
                    "summary": {
                        "attempted_orders": 1,
                        "filled_orders": 1,
                        "partial_orders": 0,
                        "filled_size": "10.0000000000",
                        "filled_notional": "5.1000000000",
                        "source_fill_count": 1,
                    }
                }
            },
        },
    )
    token_id = str(int(asset_id, 16))
    monkeypatch.setattr(
        "quant.backtest.timestamp_native_position_adapter._token_mappings",
        lambda _conn, token_ids: {
            token_id: {
                "market_id": 7,
                "condition_id": "0x" + "c" * 64,
                "outcome": "YES",
                "mapping_sources": ("fixture",),
            }
        },
    )
    output = tmp_path / "positions"

    manifest = export_timestamp_native_execution_positions(
        object(), execution_root=root, profile=profile, output_dir=output
    )
    _, positions = load_fill_only_execution_positions(output)

    assert manifest["position_count"] == 1
    assert positions[0].market_id == 7
    assert positions[0].outcome == "YES"
    assert positions[0].source_fill_ids == ()


def test_authoritative_token_table_wins_over_stale_placeholder_shortcut() -> None:
    mapping = _resolve_token_candidates(
        "123",
        {
            (10, "0x" + "a" * 64, "YES", "core.markets_shortcut_tokens"),
            (20, "0x" + "b" * 64, "YES", "core.market_tokens"),
            (20, "0x" + "b" * 64, "YES", "core.markets_shortcut_tokens"),
        },
    )

    assert mapping == {
        "market_id": 20,
        "condition_id": "0x" + "b" * 64,
        "outcome": "YES",
        "mapping_sources": ("core.market_tokens",),
    }


def test_position_uses_full_block_span_when_match_time_order_differs() -> None:
    asset_id = "a" * 64
    result = _condensed_positive_result(
        {
            "schema_version": "PMQ073TimestampNativeMatcherResultV2",
            "profile": "central",
            "signal_day": "2026-02-01",
            "global_order_ordinal": 1,
            "matcher_result": {
                "order_id": f"strategy:2026-02-01T00:00:00Z:{asset_id}:buy",
                "side": "BUY",
                "status": "FILLED",
                "filled_size": "2",
                "filled_notional": "1.0000000000",
                "fills": [
                    {
                        "filled_size": "1",
                        "exec_price": "0.5",
                        "fill_block": 902,
                        "fill_ts": "2026-02-01T00:00:01Z",
                        "source_trade_id": "source-1",
                    },
                    {
                        "filled_size": "1",
                        "exec_price": "0.5",
                        "fill_block": 901,
                        "fill_ts": "2026-02-01T00:00:02Z",
                        "source_trade_id": "source-2",
                    },
                ],
            },
        },
        profile="central",
        day="2026-02-01",
    )

    assert result is not None
    assert result["first_fill_block"] == 901
    assert result["last_fill_block"] == 902
