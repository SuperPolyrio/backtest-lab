from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest import financial_finalization_service
from quant.backtest.financial_finalization_service import (
    _fill_evidence,
    finalize_registered_backtest_run,
)
from quant.backtest.market_settlement import (
    EXACT_HEADER_REQUIRED,
    ORACLE_FINALIZED_REQUIRED,
    SETTLED_YES,
    MarketSettlementRecord,
)

UTC = timezone.utc
CUTOFF = datetime(2026, 8, 25, 13, 14, 50, tzinfo=UTC)


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def execute(self, *_args, **_kwargs) -> None:
        return None


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


def _settlement() -> MarketSettlementRecord:
    return MarketSettlementRecord(
        market_id=7,
        condition_id="0x" + "a" * 64,
        slug="fixture",
        title="Fixture",
        category="fixture",
        cutoff_ts=CUTOFF,
        classification=SETTLED_YES,
        classification_reason="fixture",
        completion_status="SETTLED",
        settlement_code=1,
        settlement_outcome="YES",
        settlement_source="fixture",
        protocol_finalized_at=CUTOFF,
        protocol_finalized_block=100,
        settlement_event_id=1,
        settlement_tx_hash="0x" + "b" * 64,
        tokens=(
            {"outcome": "YES", "token_id": "1"},
            {"outcome": "NO", "token_id": "2"},
        ),
        payout_by_token={"1": Decimal(1), "2": Decimal(0)},
        evidence={"fixture": True},
        record_sha256="c" * 64,
    )


def _patch_finalizer_dependencies(monkeypatch, captured: dict[str, object]) -> None:
    monkeypatch.setattr(
        financial_finalization_service,
        "_run_and_market",
        lambda *_args: (
            {"run_id": 9, "meta": {}, "status": "succeeded"},
            {"market_id": 7},
        ),
    )
    monkeypatch.setattr(
        financial_finalization_service,
        "_open_positions",
        lambda *_args, **_kwargs: ("e" * 64, ()),
    )
    monkeypatch.setattr(
        financial_finalization_service,
        "get_backtest_financials",
        lambda *_args, **_kwargs: None,
    )

    def replay(*_args, **kwargs):
        captured["replay"] = kwargs
        return iter([_settlement()])

    monkeypatch.setattr(
        financial_finalization_service,
        "iter_market_settlement_catalog",
        replay,
    )

    def finalize(*_args, **kwargs):
        captured["required"] = kwargs["required_evidence_grade"]
        return ({"status": "N/A_NO_DEPLOYED_CAPITAL"}, ())

    monkeypatch.setattr(
        financial_finalization_service,
        "finalize_fill_only_positions",
        finalize,
    )


def test_fill_evidence_rejects_naive_timestamp() -> None:
    assert (
        _fill_evidence(
            {
                "orderfilled_v2": {
                    "fills": [
                        {
                            "fill_ts": "2026-08-01T12:00:00",
                            "fill_block": 100,
                            "source_trade_id": "trade-1",
                        }
                    ]
                }
            }
        )
        == ()
    )


def test_fill_evidence_accepts_utc_timestamp() -> None:
    rows = _fill_evidence(
        {
            "orderfilled_v2": {
                "fills": [
                    {
                        "fill_ts": "2026-08-01T12:00:00Z",
                        "fill_block": 100,
                        "source_trade_id": "trade-1",
                    }
                ]
            }
        }
    )

    assert len(rows) == 1
    assert rows[0][0].isoformat() == "2026-08-01T12:00:00+00:00"
    assert rows[0][1:] == (100, "trade-1")


def test_registered_finalizer_defaults_to_normal_oracle_policy(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _patch_finalizer_dependencies(monkeypatch, captured)
    monkeypatch.setattr(
        financial_finalization_service,
        "resolve_polygon_cutoff_boundary",
        lambda *_args, **_kwargs: {"boundary_sha256": "b" * 64},
    )

    result = finalize_registered_backtest_run(
        _Conn(),
        9,
        {"cutoffTs": CUTOFF.isoformat()},
        clickhouse=object(),
    )

    replay = captured["replay"]
    assert isinstance(replay, dict)
    assert replay["evidence_policy"] == ORACLE_FINALIZED_REQUIRED
    assert replay["cutoff_block_boundary"] == {"boundary_sha256": "b" * 64}
    assert replay["rpc_backfill_missing_headers"] is False
    assert captured["required"] == "oracle-finalized"
    assert result["status"] == "N/A_NO_DEPLOYED_CAPITAL"


def test_registered_finalizer_keeps_exact_header_as_explicit_audit(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}
    _patch_finalizer_dependencies(monkeypatch, captured)
    monkeypatch.setattr(
        financial_finalization_service,
        "resolve_polygon_cutoff_boundary",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("exact audit must not resolve a cutoff boundary")
        ),
    )

    finalize_registered_backtest_run(
        _Conn(),
        9,
        {"cutoffTs": CUTOFF.isoformat(), "evidencePolicy": "exact-header"},
        clickhouse=object(),
    )

    replay = captured["replay"]
    assert isinstance(replay, dict)
    assert replay["evidence_policy"] == EXACT_HEADER_REQUIRED
    assert replay["cutoff_block_boundary"] is None
    assert replay["rpc_backfill_missing_headers"] is True
    assert captured["required"] == "exact-header"
