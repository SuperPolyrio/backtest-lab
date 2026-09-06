from __future__ import annotations

from pathlib import Path

import pytest
from flask import Flask

from quant.backtest.fill_only_v2_service import (
    FillOnlyV2AnchorError,
    FillOnlyV2CoverageError,
    FillOnlyV2LimitError,
    FillOnlyV2RequestError,
    list_fill_only_v2_profiles,
    resolve_fill_only_v2_anchor,
    run_fill_only_v2_replay,
)
from quant.backtest.rust_kernel import rust_kernel_available
from scripts.api.routes import quant as quant_routes


class FakeClickHouse:
    def __init__(
        self,
        trades: list[dict] | None = None,
        anchor_rows: list[dict] | None = None,
        coverage_rows: list[dict] | None = None,
    ) -> None:
        self.trades = list(trades or [])
        self.anchor_rows = list(anchor_rows or [])
        self.coverage_rows = list(coverage_rows or [])
        self.queries: list[str] = []

    def query_json_rows(self, query: str, *, timeout_seconds: float | None = None) -> list[dict]:
        self.queries.append(query)
        if "orderfilled_v2_build_chunks" in query:
            return [
                {
                    "from_block": 1,
                    "to_block": 1_000,
                    "build_tag": "test-v2-build",
                }
            ]
        if "'coverage_start' AS relation" in query:
            return list(self.coverage_rows)
        if "'before' AS relation" in query:
            return list(self.anchor_rows)
        if "trade_prints_one_sided" in query:
            return list(self.trades)
        raise AssertionError(f"unexpected query: {query}")


def _trade(*, trade_id: str, block: int, block_time: str, price: str = "0.55", size: str = "100") -> dict:
    return {
        "trade_id": trade_id,
        "market_id": 42,
        "condition_id": "0xcondition",
        "asset_id": "asset-yes",
        "outcome": "YES",
        "block_number": block,
        "block_time": block_time,
        "tx_hash": f"0x{trade_id}",
        "tx_index": 1,
        "tx_index_source": "receipt",
        "price": price,
        "size_shares": size,
        "notional_usdc": "55",
        "aggressor_side": "BUY",
        "passive_side": "SELL",
        "source_log_indexes": [1],
        "source_fill_count": 1,
    }


def _payload(**overrides) -> dict:
    payload = {
        "requestId": "alpha-formal-v2-001",
        "profile": "conservative_trade_tape",
        "sourceMaxBlock": 1_000,
        "defaultLookbackBlocks": 20,
        "defaultHorizonBlocks": 20,
        "maxRowsPerWindow": 100,
        "orders": [
            {
                "orderId": "intent-1",
                "marketId": 42,
                "assetId": "asset-yes",
                "side": "BUY",
                "limitPrice": "0.60",
                "size": "10",
                "signalBlock": 100,
                "signalTs": "2026-06-01T12:00:00Z",
                "tif": "GTC",
                "allowPartialFill": True,
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_fill_only_service_loads_bounded_trade_slice_and_returns_auditable_fill() -> None:
    clickhouse = FakeClickHouse([
        _trade(trade_id="trailing", block=99, block_time="2026-06-01T11:59:59Z"),
        _trade(trade_id="eligible", block=105, block_time="2026-06-01T12:00:02Z"),
    ])

    result = run_fill_only_v2_replay(_payload(), client=clickhouse)

    assert result["schema_version"] == "fill-only-v2-replay-api-v1"
    assert result["uses_lob_data"] is False
    assert result["source_max_block"] == 1_000
    assert result["trade_slice_load"]["rows_loaded"] == 2
    assert result["orders"][0]["status"] == "PARTIAL_FILLED"
    assert result["orders"][0]["fills"][0]["source_trade_id"] == "eligible"
    assert result["capacity_ledger"]["eligible"] == "2.5000000000"
    assert result["manifest"]["orders"][0]["trade_slice_lookback_blocks"] == 20
    assert all("lob" not in query.lower() for query in clickhouse.queries)


@pytest.mark.skipif(not rust_kernel_available(), reason="Rust extension is not installed")
def test_fill_only_v2_service_exposes_explicit_rust_matcher() -> None:
    clickhouse = FakeClickHouse(
        [_trade(trade_id="eligible", block=105, block_time="2026-06-01T12:00:02Z")]
    )
    result = run_fill_only_v2_replay(
        _payload(profile="optimistic_sensitivity", matcherBackend="rust"),
        client=clickhouse,
    )

    assert result["match_diagnostics"]["matching_backend"] == "rust"
    assert result["manifest"]["matcher_backend"] == "rust"


def test_fill_only_v2_service_rejects_unknown_matcher_backend_before_query() -> None:
    clickhouse = FakeClickHouse()
    with pytest.raises(FillOnlyV2RequestError, match="matcher_backend"):
        run_fill_only_v2_replay(
            _payload(matcherBackend="cuda"),
            client=clickhouse,
        )
    assert clickhouse.queries == []


def test_fill_only_service_accepts_signal_ts_only_and_loads_time_slice() -> None:
    trades = [
        _trade(trade_id="trailing", block=99, block_time="2026-06-01T11:59:59Z"),
        _trade(trade_id="eligible", block=105, block_time="2026-06-01T12:00:02Z"),
    ]
    clickhouse = FakeClickHouse(
        trades,
        coverage_rows=[
            {
                "relation": "coverage_start",
                "trade_id": "coverage-before",
                "block_number": 1,
                "block_time": "2026-05-01T00:00:00Z",
            },
            {
                "relation": "coverage_end",
                "trade_id": "coverage-after",
                "block_number": 1_000,
                "block_time": "2026-07-01T00:00:00Z",
            },
        ],
    )
    payload = _payload(defaultLookbackSeconds=20, defaultHorizonSeconds=20)
    del payload["orders"][0]["signalBlock"]

    result = run_fill_only_v2_replay(payload, client=clickhouse)

    assert result["orders"][0]["status"] == "PARTIAL_FILLED"
    assert result["orders"][0]["fills"][0]["source_trade_id"] == "eligible"
    assert result["manifest"]["orders"][0]["signal_block"] is None
    assert result["manifest"]["orders"][0]["arrival_block"] is None
    assert result["manifest"]["orders"][0]["deadline_block"] is None
    assert result["manifest"]["orders"][0]["arrival_ts"] == "2026-06-01T12:00:01+00:00"
    assert result["manifest"]["orders"][0]["deadline_ts"] == "2026-06-01T12:00:21+00:00"
    assert result["time_coverage_envelope"]["validation_method"] == "run_level_trade_time_envelope"
    assert result["time_coverage_envelope"]["is_order_anchor"] is False
    slice_queries = [
        query
        for query in clickhouse.queries
        if "FROM trade_prints_one_sided" in query and "block_time BETWEEN" in query
    ]
    assert len(slice_queries) == 1
    assert "block_number BETWEEN 1 AND 1000" in slice_queries[0]
    assert "block_time BETWEEN" in slice_queries[0]
    assert all("interpol" not in query.lower() for query in clickhouse.queries)


def test_fill_only_signal_ts_only_preserves_request_order_and_shared_capacity() -> None:
    clickhouse = FakeClickHouse(
        [
            _trade(trade_id="trailing", block=99, block_time="2026-06-01T11:59:59Z"),
            _trade(trade_id="shared", block=105, block_time="2026-06-01T12:00:02Z"),
        ],
        coverage_rows=[
            {
                "relation": "coverage_start",
                "trade_id": "coverage-before",
                "block_number": 1,
                "block_time": "2026-05-01T00:00:00Z",
            },
            {
                "relation": "coverage_end",
                "trade_id": "coverage-after",
                "block_number": 1_000,
                "block_time": "2026-07-01T00:00:00Z",
            },
        ],
    )
    payload = _payload(defaultLookbackSeconds=20, defaultHorizonSeconds=20)
    first = payload["orders"][0]
    del first["signalBlock"]
    first["size"] = "2"
    payload["orders"] = [first, {**first, "orderId": "intent-2"}]

    result = run_fill_only_v2_replay(payload, client=clickhouse)

    assert [row["order_id"] for row in result["orders"]] == ["intent-1", "intent-2"]
    assert result["orders"][0]["filled_size"] == "2.0000000000"
    assert result["orders"][1]["filled_size"] == "0.5000000000"
    assert result["capacity_ledger"]["shared"] == "2.5000000000"


def test_fill_only_signal_ts_only_fails_closed_without_time_envelope() -> None:
    payload = _payload(defaultLookbackSeconds=20, defaultHorizonSeconds=20)
    del payload["orders"][0]["signalBlock"]

    with pytest.raises(FillOnlyV2CoverageError, match="not enclosed"):
        run_fill_only_v2_replay(payload, client=FakeClickHouse())


def test_fill_only_service_rejects_request_past_derived_coverage() -> None:
    payload = _payload(sourceMaxBlock=1_000)
    payload["orders"][0]["signalBlock"] = 995

    try:
        run_fill_only_v2_replay(payload, client=FakeClickHouse())
    except FillOnlyV2CoverageError as exc:
        assert "beyond pinned source_max_block" in str(exc)
    else:
        raise AssertionError("coverage gap must fail closed")


def test_fill_only_service_rejects_dense_slice_instead_of_silent_truncation() -> None:
    clickhouse = FakeClickHouse([
        _trade(trade_id="trade-1", block=99, block_time="2026-06-01T11:59:59Z"),
        _trade(trade_id="trade-2", block=105, block_time="2026-06-01T12:00:02Z"),
    ])

    try:
        run_fill_only_v2_replay(_payload(maxRowsPerWindow=1), client=clickhouse)
    except FillOnlyV2LimitError as exc:
        assert "trade slice exceeds row limit" in str(exc)
    else:
        raise AssertionError("truncated trade slices must fail closed")


@pytest.mark.parametrize(
    "field",
    ["lookbackBlocks", "defaultLookbackBlock", "unexpected"],
)
def test_fill_only_service_rejects_unknown_request_fields(field: str) -> None:
    payload = _payload()
    payload[field] = 100

    with pytest.raises(FillOnlyV2RequestError, match="request contains unknown fields"):
        run_fill_only_v2_replay(payload, client=FakeClickHouse())


def test_fill_only_service_rejects_unknown_order_fields_and_alias_conflicts() -> None:
    payload = _payload()
    payload["orders"][0]["limitPrce"] = "0.60"
    with pytest.raises(FillOnlyV2RequestError, match="order contains unknown fields: limitPrce"):
        run_fill_only_v2_replay(payload, client=FakeClickHouse())

    payload = _payload()
    payload["orders"][0]["lookback_blocks"] = 20
    payload["orders"][0]["lookbackBlocks"] = 20
    with pytest.raises(FillOnlyV2RequestError, match="order contains conflicting aliases"):
        run_fill_only_v2_replay(payload, client=FakeClickHouse())


def test_resolve_anchor_uses_causal_trade_tape_bracket() -> None:
    clickhouse = FakeClickHouse(anchor_rows=[
        {
            "relation": "before",
            "trade_id": "before-trade",
            "block_number": 100,
            "block_time": "2026-06-01 11:59:58",
        },
        {
            "relation": "after",
            "trade_id": "after-trade",
            "block_number": 102,
            "block_time": "2026-06-01 12:00:02",
        },
    ])

    result = resolve_fill_only_v2_anchor(
        {
            "requestId": "anchor-1",
            "signalTs": "2026-06-01T12:00:00Z",
            "sourceMaxBlock": 1_000,
            "maxDistanceSeconds": 10,
        },
        client=clickhouse,
    )

    assert result["anchor_block"] == 101
    assert result["resolution_method"] == "interpolated_trade_bracket"
    assert result["coverage_eligible"] is True
    assert result["bracket"]["before"]["trade_id"] == "before-trade"
    assert result["bracket"]["after"]["trade_id"] == "after-trade"
    assert all("block_timestamps" not in query for query in clickhouse.queries)


def test_resolve_anchor_fails_closed_without_nearby_two_sided_bracket() -> None:
    clickhouse = FakeClickHouse(anchor_rows=[
        {
            "relation": "before",
            "trade_id": "old-trade",
            "block_number": 100,
            "block_time": "2026-06-01 11:00:00",
        },
    ])

    with pytest.raises(FillOnlyV2AnchorError, match="cannot be bracketed"):
        resolve_fill_only_v2_anchor(
            {"signalTs": "2026-06-01T12:00:00Z", "sourceMaxBlock": 1_000},
            client=clickhouse,
        )


def test_fill_only_profiles_exclude_lob_calibrated_runtime_contract() -> None:
    profiles = {row["name"]: row for row in list_fill_only_v2_profiles()}
    names = set(profiles)
    assert "conservative_trade_tape" in names
    assert "probabilistic_trade_tape" in names
    assert "lob_holdout_calibrated_fill_only" not in names
    expected = profiles["probabilistic_trade_tape"]
    assert expected["probability_enabled"] is True
    assert expected["probability_model_version"] == "orderfilled_arrival_and_conditional_capacity_v2"
    assert expected["capacity_variant"] == "expected"
    assert expected["hard_reject_below_probability"] is False
    assert expected["fills_require_source_trade"] is True
    assert profiles["probabilistic_conservative"]["capacity_variant"] == "conservative"
    assert profiles["probabilistic_source_confirmed"]["capacity_variant"] == "source_confirmed"
    assert profiles["probabilistic_taker_120s_any_order_side"]["trade_side_evidence_mode"] == "any_order_side"


def test_fill_only_http_routes_expose_profiles_readiness_and_replay(monkeypatch) -> None:
    monkeypatch.setattr(
        quant_routes,
        "load_trade_tape_coverage",
        lambda: type(
            "Coverage",
            (),
            {"as_dict": lambda self: {"source_table": "trade_prints_one_sided", "max_block": 1_000}},
        )(),
    )
    monkeypatch.setattr(
        quant_routes,
        "run_fill_only_v2_replay",
        lambda payload: {
            "schema_version": "fill-only-v2-replay-api-v1",
            "request_id": payload["requestId"],
            "uses_lob_data": False,
            "orders": [],
        },
    )
    monkeypatch.setattr(
        quant_routes,
        "resolve_fill_only_v2_anchor",
        lambda payload: {
            "schema_version": "fill-only-v2-replay-api-v1",
            "request_id": payload["requestId"],
            "uses_lob_data": False,
            "anchor_block": 101,
        },
    )
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))
    client = app.test_client()

    profiles = client.get("/quant/fill-only/v2/profiles")
    readiness = client.get("/quant/fill-only/v2/readiness")
    replay = client.post("/quant/fill-only/v2/replay", json={"requestId": "request-1"})
    anchor = client.post("/quant/fill-only/v2/resolve-anchor", json={"requestId": "anchor-1"})

    assert profiles.status_code == 200
    assert profiles.get_json()["uses_lob_data"] is False
    assert readiness.status_code == 200
    assert readiness.get_json()["source_coverage"]["max_block"] == 1_000
    assert replay.status_code == 200
    assert replay.get_json()["request_id"] == "request-1"
    assert anchor.status_code == 200
    assert anchor.get_json()["anchor_block"] == 101


def test_fill_only_http_route_preserves_domain_error_status(monkeypatch) -> None:
    def fail(_payload):
        raise FillOnlyV2CoverageError("derived tape stops before requested order")

    monkeypatch.setattr(quant_routes, "run_fill_only_v2_replay", fail)
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))

    response = app.test_client().post("/quant/fill-only/v2/replay", json={"orders": [{}]})

    assert response.status_code == 409
    assert response.get_json()["error_code"] == "TRADE_TAPE_COVERAGE_GAP"


def test_fill_only_http_route_rejects_unknown_json_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        quant_routes,
        "run_fill_only_v2_replay",
        lambda payload: run_fill_only_v2_replay(payload, client=FakeClickHouse()),
    )
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))
    client = app.test_client()

    top_level = client.post(
        "/quant/fill-only/v2/replay",
        json={**_payload(), "lookbackBlocks": 100},
    )
    nested_payload = _payload()
    nested_payload["orders"][0]["limitPrce"] = "0.60"
    nested = client.post("/quant/fill-only/v2/replay", json=nested_payload)

    assert top_level.status_code == 400
    assert top_level.get_json()["error_code"] == "INVALID_FILL_ONLY_REQUEST"
    assert "lookbackBlocks" in top_level.get_json()["error"]
    assert nested.status_code == 400
    assert "limitPrce" in nested.get_json()["error"]


def test_resolve_anchor_http_route_rejects_unknown_json_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        quant_routes,
        "resolve_fill_only_v2_anchor",
        lambda payload: resolve_fill_only_v2_anchor(payload, client=FakeClickHouse()),
    )
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))

    response = app.test_client().post(
        "/quant/fill-only/v2/resolve-anchor",
        json={"signalTs": "2026-06-01T12:00:00Z", "signalTime": "typo"},
    )

    assert response.status_code == 400
    assert response.get_json()["error_code"] == "INVALID_FILL_ONLY_REQUEST"
    assert "signalTime" in response.get_json()["error"]


def test_fill_only_api_doc_pins_real_acceptance_window() -> None:
    project_root = Path(__file__).resolve().parents[3]
    markdown = (project_root / "docs" / "api" / "fill-only-v2-replay-api.md").read_text(encoding="utf-8")

    assert '"defaultLookbackBlocks": 100' in markdown
    assert '"defaultHorizonBlocks": 100' in markdown
    assert "5.3728152 / 10" in markdown
    assert "15" in markdown
