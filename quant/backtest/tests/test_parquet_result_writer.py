from __future__ import annotations

import json

import pyarrow.dataset as ds
import pytest

from quant.backtest.parquet_result_writer import BoundedParquetResultWriter


def _result(index: int, *, modeled: bool = False) -> dict:
    return {
        "order_id": f"order-{index}",
        "status": "MODELED_EXPECTATION" if modeled else "PARTIAL_FILLED",
        "filled_size": "1.25",
        "avg_price": "0.55",
        "reason": "modeled" if modeled else "capacity",
        "evidence_tier": "D_SYNTHETIC_ARRIVAL" if modeled else "A_SOURCE_CONFIRMED",
        "result_role": "CENTRAL_RESEARCH" if modeled else "AUDIT_LOWER_BOUND",
        "fills": [
            {
                "source_trade_ids": [] if modeled else [f"trade-{index}"],
                "filled_size": "1.25",
            }
        ],
    }


def _rows(root) -> list[dict]:
    paths = sorted(str(path) for path in root.glob("part-*.parquet"))
    return ds.dataset(paths, format="parquet").to_table().to_pylist()


def test_research_writer_batches_and_omits_large_audit_payload(tmp_path) -> None:
    root = tmp_path / "research"
    writer = BoundedParquetResultWriter(
        root,
        run_id="run",
        profile_hash="profile",
        strategy_hash="strategy",
        source_pin="source",
        mode="research",
        batch_size=2,
        queue_size=2,
    )
    for index in range(5):
        writer.submit(
            _result(index, modeled=index == 4),
            profile="central",
            high_watermark={"trade_id": f"trade-{index}"},
        )
    manifest = writer.close()

    rows = _rows(root)
    assert len(rows) == 5
    assert len(list(root.glob("part-*.parquet"))) == 3
    assert all(row["result_json"] is None for row in rows)
    assert all(row["source_fills_json"] is None for row in rows)
    assert rows[-1]["is_modeled"] is True
    assert manifest["rows_written"] == 5
    assert manifest["high_watermark"] == {"trade_id": "trade-4"}
    assert json.loads((root / "checkpoint.json").read_text())["rows_written"] == 5


def test_audit_writer_preserves_full_result_and_source_fills(tmp_path) -> None:
    root = tmp_path / "audit"
    with BoundedParquetResultWriter(
        root,
        run_id="run",
        profile_hash="profile",
        strategy_hash="strategy",
        source_pin="source",
        mode="audit",
        batch_size=10,
    ) as writer:
        writer.submit(_result(1), profile="source")

    row = _rows(root)[0]
    assert json.loads(row["result_json"])["order_id"] == "order-1"
    assert json.loads(row["source_fills_json"])[0]["source_trade_ids"] == [
        "trade-1"
    ]


def test_writer_resume_continues_from_atomic_checkpoint(tmp_path) -> None:
    root = tmp_path / "resume"
    first = BoundedParquetResultWriter(
        root,
        run_id="run",
        profile_hash="profile",
        strategy_hash="strategy",
        source_pin="source",
        batch_size=2,
    )
    first.submit(_result(0), profile="source", high_watermark={"trade_id": "t0"})
    first.submit(_result(1), profile="source", high_watermark={"trade_id": "t1"})
    first.close(finalize=False)

    resumed = BoundedParquetResultWriter(
        root,
        run_id="run",
        profile_hash="profile",
        strategy_hash="strategy",
        source_pin="source",
        batch_size=2,
        resume=True,
    )
    resumed.submit(_result(2), profile="source", high_watermark={"trade_id": "t2"})
    manifest = resumed.close()

    assert [row["order_id"] for row in _rows(root)] == [
        "order-0",
        "order-1",
        "order-2",
    ]
    assert manifest["rows_written"] == 3
    assert manifest["high_watermark"] == {"trade_id": "t2"}


def test_writer_resume_rejects_changed_run_binding(tmp_path) -> None:
    root = tmp_path / "binding"
    writer = BoundedParquetResultWriter(
        root,
        run_id="run",
        profile_hash="profile",
        strategy_hash="strategy",
        source_pin="source",
        batch_size=1,
    )
    writer.submit(_result(0), profile="source")
    writer.close(finalize=False)

    with pytest.raises(ValueError, match="profile_hash mismatch"):
        BoundedParquetResultWriter(
            root,
            run_id="run",
            profile_hash="changed",
            strategy_hash="strategy",
            source_pin="source",
            resume=True,
        )
