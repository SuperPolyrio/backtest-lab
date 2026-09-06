from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from flask import Flask

from quant.backtest.market_settlement import (
    ORACLE_FINALIZED_REQUIRED,
    SETTLED_YES,
    MarketSettlementRecord,
)
from quant.backtest.market_settlement_service import (
    MarketSettlementServiceError,
    resolve_market_settlements,
)
from scripts.api.routes import quant as quant_routes

UTC = timezone.utc
CUTOFF = datetime(2026, 8, 25, 13, 14, 50, tzinfo=UTC)


def _record() -> MarketSettlementRecord:
    return MarketSettlementRecord(
        market_id=7,
        condition_id="0x" + "a" * 64,
        slug="market-7",
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


def test_settlement_service_rejects_unknown_fields() -> None:
    with pytest.raises(MarketSettlementServiceError) as error:
        resolve_market_settlements(
            object(),
            {
                "marketIds": [7],
                "cutoffTs": CUTOFF.isoformat(),
                "cuttofTs": "typo",
            },
        )

    assert error.value.error_code == "UNKNOWN_SETTLEMENT_FIELD"
    assert error.value.status_code == 400


def test_settlement_service_resolves_market_batch(monkeypatch) -> None:
    persisted: dict[str, object] = {}
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "quant.backtest.market_settlement_service.resolve_polygon_cutoff_boundary",
        lambda *_args, **_kwargs: {"boundary_sha256": "b" * 64},
    )

    def replay(*_args, **kwargs):
        captured.update(kwargs)
        return iter([_record()])

    monkeypatch.setattr(
        "quant.backtest.market_settlement_service.iter_market_settlement_catalog",
        replay,
    )
    monkeypatch.setattr(
        "quant.backtest.market_settlement_service._persist_records",
        lambda _conn, **kwargs: persisted.update(kwargs),
    )

    result = resolve_market_settlements(
        object(),
        {"marketIds": [7], "cutoffTs": CUTOFF.isoformat()},
    )

    assert result["market_count"] == 1
    assert result["classification_counts"] == {SETTLED_YES: 1}
    assert result["evidence_policy"] == ORACLE_FINALIZED_REQUIRED
    assert captured["evidence_policy"] == ORACLE_FINALIZED_REQUIRED
    assert captured["rpc_backfill_missing_headers"] is False
    assert persisted["cutoff_ts"] == CUTOFF


def test_settlement_service_exposes_scalable_cutoff_block_policy(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "quant.backtest.market_settlement_service.resolve_polygon_cutoff_boundary",
        lambda *_args, **_kwargs: {"boundary_sha256": "b" * 64},
    )

    def replay(*_args, **kwargs):
        captured.update(kwargs)
        return iter([_record()])

    monkeypatch.setattr(
        "quant.backtest.market_settlement_service.iter_market_settlement_catalog",
        replay,
    )

    result = resolve_market_settlements(
        object(),
        {
            "marketIds": [7],
            "cutoffTs": CUTOFF.isoformat(),
            "evidencePolicy": "oracle-event-cutoff-block",
            "persist": False,
        },
    )

    assert result["evidence_policy"] == "ORACLE_EVENT_CUTOFF_BLOCK"
    assert captured["rpc_backfill_missing_headers"] is False
    assert captured["cutoff_block_boundary"] == {"boundary_sha256": "b" * 64}


class _FakeConn:
    committed = False

    def commit(self) -> None:
        self.committed = True


class _ConnectionContext:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def __enter__(self) -> _FakeConn:
        return self.conn

    def __exit__(self, *_args) -> bool:
        return False


def test_market_settlement_http_route_is_strategy_independent(monkeypatch) -> None:
    conn = _FakeConn()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        quant_routes,
        "postgres_connection",
        lambda *_args, **_kwargs: _ConnectionContext(conn),
    )

    def resolve(_conn, payload, **_kwargs):
        captured.update(payload)
        return {
            "market_count": 2,
            "classification_counts": {SETTLED_YES: 2},
            "records": [],
        }

    monkeypatch.setattr(quant_routes, "resolve_market_settlements", resolve)
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))

    response = app.test_client().post(
        "/quant/fill-only/settlements/resolve",
        json={"marketIds": [7, 8], "cutoffTs": CUTOFF.isoformat()},
    )

    assert response.status_code == 200
    assert response.get_json()["item"]["marketCount"] == 2
    assert captured["marketIds"] == [7, 8]
    assert conn.committed is True
