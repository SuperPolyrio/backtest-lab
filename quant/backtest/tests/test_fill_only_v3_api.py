from __future__ import annotations

import pytest
from flask import Flask

from quant.backtest.rust_kernel import rust_kernel_available
from quant.backtest.trade_only_v3 import service as v3_service
from quant.backtest.trade_only_v3.service import (
    FillOnlyV3CoverageError,
    FillOnlyV3RequestError,
    list_fill_only_v3_profiles,
    resolve_fill_only_v3_anchor,
    run_fill_only_v3_replay,
)
from scripts.api.routes import quant as quant_routes


class FakeClickHouse:
    def __init__(self, trades: list[dict] | None = None) -> None:
        self.trades = list(trades or [])
        self.queries: list[str] = []

    def query_json_rows(
        self, query: str, *, timeout_seconds: float | None = None
    ) -> list[dict]:
        self.queries.append(query)
        if "orderfilled_v2_build_chunks" in query:
            return [{"from_block": 1, "to_block": 1_000, "build_tag": "test-v3"}]
        if "trade_prints_one_sided" in query:
            return list(self.trades)
        raise AssertionError(f"unexpected query: {query}")


def _trade(
    trade_id: str, *, block: int, block_time: str, side: str, price: str
) -> dict:
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
        "size_shares": "100",
        "notional_usdc": "55",
        "aggressor_side": side,
        "passive_side": "SELL" if side == "BUY" else "BUY",
        "source_log_indexes": [1],
        "source_fill_count": 1,
    }


def _payload(**overrides) -> dict:
    payload = {
        "requestId": "v3-test",
        "profile": "taker_synthetic_q50",
        "sourceMaxBlock": 1_000,
        "defaultLookbackBlocks": 20,
        "defaultHorizonBlocks": 20,
        "maxRowsPerWindow": 100,
        "randomSeed": 73,
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
                "tif": "IOC",
                "liquidityIntent": "TAKER",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_v3_service_returns_model_inferred_fill_without_future_trade() -> None:
    clickhouse = FakeClickHouse(
        [
            _trade(
                "pre-buy",
                block=98,
                block_time="2026-06-01T11:59:57Z",
                side="BUY",
                price="0.55",
            ),
            _trade(
                "pre-sell",
                block=99,
                block_time="2026-06-01T11:59:58Z",
                side="SELL",
                price="0.53",
            ),
        ]
    )

    result = run_fill_only_v3_replay(_payload(), client=clickhouse)

    assert result["schema_version"] == "fill-only-v3-trade-only-api-v1"
    assert result["uses_lob_data"] is False
    assert result["orders"][0]["fills"][0]["source_trade_ids"] == []
    assert result["orders"][0]["fills"][0]["evidence_tier"] == "D_SYNTHETIC_ARRIVAL"
    assert result["warnings"]
    assert all("lob" not in query.lower() for query in clickhouse.queries)


def test_v3_service_normalizes_decimal_polymarket_token_for_clickhouse() -> None:
    trade = _trade(
        "pre-buy",
        block=99,
        block_time="2026-06-01T11:59:58Z",
        side="BUY",
        price="0.55",
    )
    trade["asset_id"] = "0" * 63 + "1"
    payload = _payload()
    payload["orders"][0]["assetId"] = "1"
    clickhouse = FakeClickHouse([trade])

    result = run_fill_only_v3_replay(payload, client=clickhouse)

    assert result["manifest"]["orders"][0]["asset_id"] == "0" * 63 + "1"
    assert any(
        "asset_id = '" + "0" * 63 + "1'" in query for query in clickhouse.queries
    )


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_v3_service_exposes_rust_for_source_confirmed_profile() -> None:
    clickhouse = FakeClickHouse(
        [
            _trade(
                "future-buy",
                block=105,
                block_time="2026-06-01T12:00:02Z",
                side="BUY",
                price="0.55",
            )
        ]
    )
    result = run_fill_only_v3_replay(
        _payload(
            profile="taker_source_confirmed",
            matcherBackend="rust",
        ),
        client=clickhouse,
    )

    assert result["match_diagnostics"]["matching_backend"] == "rust"
    assert result["manifest"]["matcher_backend"] == "rust"


def test_v3_service_rejects_unknown_matcher_backend_before_query() -> None:
    clickhouse = FakeClickHouse()
    with pytest.raises(FillOnlyV3RequestError, match="matcher_backend"):
        run_fill_only_v3_replay(
            _payload(matcherBackend="gpu"),
            client=clickhouse,
        )
    assert clickhouse.queries == []


def test_v3_service_rejects_unknown_fields_and_coverage_gaps() -> None:
    with pytest.raises(FillOnlyV3RequestError, match="unknown fields"):
        run_fill_only_v3_replay(
            {**_payload(), "lookbackBlocks": 20}, client=FakeClickHouse()
        )

    payload = _payload()
    payload["orders"][0]["signalBlock"] = 1_000
    with pytest.raises(FillOnlyV3CoverageError, match="does not cover"):
        run_fill_only_v3_replay(payload, client=FakeClickHouse())


def test_v3_service_accepts_single_name_market_context_fields() -> None:
    payload = _payload()
    payload["orders"][0]["category"] = "crypto"
    payload["orders"][0]["league"] = "none"

    result = run_fill_only_v3_replay(payload, client=FakeClickHouse())

    order = result["manifest"]["orders"][0]
    assert order["category"] == "crypto"
    assert order["league"] == "none"


def test_v3_profiles_expose_evidence_and_calibration_status() -> None:
    profiles = {row["name"]: row for row in list_fill_only_v3_profiles()}

    assert profiles["taker_source_confirmed"]["evidence_tier"] == "A_SOURCE_CONFIRMED"
    assert (
        profiles["maker_trade_through_lower"]["evidence_tier"]
        == "B_TRADE_THROUGH_INFERRED"
    )
    assert (
        profiles["maker_touch_survival_expected"]["result_role"] == "SENSITIVITY_ONLY"
    )
    assert (
        profiles["taker_synthetic_q50"]["calibration_status"]
        == "TAPE_PROXY_UNCALIBRATED"
    )
    assert profiles["generative_tape_mc"]["uses_lob_data"] is False
    hierarchical = profiles["taker_hierarchical_expected_120s"]
    assert hierarchical["execution_mode"] == "HIERARCHICAL_EXPECTED_FILL"
    assert hierarchical["default_tif"] == "GTD"
    assert hierarchical["default_horizon_blocks"] == 300
    assert hierarchical["minimum_pre_arrival_trades"] == 1
    assert hierarchical["probability_profile_path"].endswith(".120s.v1.json")
    assert profiles["central_trade_only_30s"]["execution_mode"] == "CENTRAL_ROUTER"
    tif_aware = profiles["central_trade_only_tif_aware_5s"]
    assert tif_aware["execution_mode"] == "CENTRAL_ROUTER"
    assert tif_aware["default_tif"] == "FAK"
    assert tif_aware["allow_immediate_tif_modeling"] is True
    assert tif_aware["allow_prior_only"] is True
    assert tif_aware["minimum_prior_samples"] == 100
    assert tif_aware["probability_model_horizon_seconds"] == 5
    assert tif_aware["minimum_modeled_probability"] == "0.10"
    recall = profiles["central_trade_only_tif_aware_5s_recall"]
    assert recall["minimum_modeled_probability"] == "0.05"
    assert recall["result_role"] == "SENSITIVITY_ONLY"
    l2_reference = profiles["central_trade_only_l2_reference_fak"]
    assert l2_reference["uses_lob_data"] is False
    assert l2_reference["default_tif"] == "FAK"
    assert l2_reference["direct_probability_model"] is True
    assert l2_reference["use_probability_profile_threshold"] is True
    l2_expected = profiles["central_trade_only_l2_reference_expected_fak"]
    assert l2_expected["uses_lob_data"] is False
    assert l2_expected["default_tif"] == "FAK"
    assert l2_expected["direct_probability_model"] is True
    assert l2_expected["use_probability_profile_threshold"] is False
    assert l2_expected["minimum_modeled_probability"] == "0"
    adaptive = profiles["central_trade_only_contract_aware_adaptive"]
    assert adaptive["uses_lob_data"] is False
    assert adaptive["enforce_probability_contract"] is True
    assert adaptive["probability_profile_path"].endswith("adaptive_v22.json")
    assert adaptive["full_fill_probability_profile_path"].endswith("adaptive_v14.json")
    assert adaptive["probability_profile_sha256"] == (
        "2d4c6b3881f02351da06dec0cb046112726cbb4efbe8435927f77c84956e3b2c"
    )
    assert adaptive["full_fill_probability_profile_sha256"] == (
        "0c158c60d91c995972f71a8574c517ce725fdd438f7591fcefb4f4dc9758a085"
    )
    assert adaptive["calibration_status"] == (
        "PML2_CONTRACT_SPECIFIC_ADAPTIVE_OFFLINE_VALIDATED"
    )


def test_v3_service_runs_adaptive_fak_and_fok_artifacts() -> None:
    payload = _payload(profile="central_trade_only_contract_aware_adaptive")
    payload["orders"][0]["tif"] = "FAK"
    fok_order = dict(payload["orders"][0])
    fok_order.update({"orderId": "intent-2", "tif": "FOK"})
    payload["orders"].append(fok_order)

    result = run_fill_only_v3_replay(payload, client=FakeClickHouse())

    fak, fok = result["orders"]
    assert fak["model_diagnostics"]["artifact_probability_target"] == "FAK_ANY_FILL"
    assert fok["model_diagnostics"]["artifact_probability_target"] == "FOK_FULL_FILL"
    assert fak["model_diagnostics"]["probability_artifact_sha256"] == (
        "2d4c6b3881f02351da06dec0cb046112726cbb4efbe8435927f77c84956e3b2c"
    )
    assert fok["model_diagnostics"]["probability_artifact_sha256"] == (
        "0c158c60d91c995972f71a8574c517ce725fdd438f7591fcefb4f4dc9758a085"
    )


def test_v3_readiness_exposes_l2_reference_orderfilled_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        v3_service,
        "build_live_order_label_readiness",
        lambda _conn: {"ready_for_live_transfer_claim": False},
    )

    readiness = v3_service.build_fill_only_v3_readiness(
        client=FakeClickHouse(), postgres_conn=object()
    )

    model = readiness["l2_reference_probability_model"]
    assert model["available"] is True
    assert model["promotion_allowed"] is True
    assert (
        model["model_version"]
        == "fill_only_v3_l2_reference_arrival_pit_aligned_grid_v2"
    )
    assert readiness["l2_reference_expected_local_research_ready"] is True
    assert readiness["capabilities"]["l2_reference_expected_fak"] is True
    assert "live_probability_calibration" in readiness
    assert readiness["ready_for_live_transfer_claim"] is False
    assert readiness["rust_kernel_available"] is rust_kernel_available()
    assert (
        readiness["profile_backends"]["central_trade_only_30s"]["backend"]
        == "HYBRID_RUST_SOURCE_PYTHON_MODEL"
    )
    assert readiness["uses_lob_data"] is False


def test_v3_service_uses_hierarchical_profile_tif_and_horizon_defaults() -> None:
    payload = _payload(profile="taker_hierarchical_expected_120s")
    payload.pop("defaultHorizonBlocks")
    payload["orders"][0].pop("tif")
    clickhouse = FakeClickHouse(
        [
            _trade(
                "pre-buy",
                block=99,
                block_time="2026-06-01T11:59:58Z",
                side="BUY",
                price="0.55",
            )
        ]
    )

    result = run_fill_only_v3_replay(payload, client=clickhouse)

    order = result["orders"][0]
    manifest_order = result["manifest"]["orders"][0]
    assert order["status"] == "MODELED_EXPECTATION"
    assert order["fills"][0]["source_trade_ids"] == []
    assert order["model_diagnostics"]["probability_training_rows"] == 23_982
    assert manifest_order["tif"] == "GTD"
    assert manifest_order["horizon_seconds"] == "120.0"
    assert manifest_order["horizon_blocks"] == 300


def test_v3_service_exposes_tif_aware_central_modeled_fallback() -> None:
    payload = _payload(profile="central_trade_only_tif_aware_5s")
    payload["marketTitle"] = "LoL: Team WE vs ThunderTalk Gaming - YES"
    payload["orders"][0].pop("tif")
    clickhouse = FakeClickHouse(
        [
            _trade(
                f"pre-buy-{index}",
                block=91 + index,
                block_time=f"2026-06-01T11:59:{50 + index:02d}Z",
                side="BUY",
                price="0.55",
            )
            for index in range(8)
        ]
    )

    result = run_fill_only_v3_replay(payload, client=clickhouse)

    order = result["orders"][0]
    assert result["profile"]["default_tif"] == "FAK"
    assert order["status"] == "MODELED_EXPECTATION"
    assert order["filled_size"] != "0E-10"
    assert order["fills"][0]["source_trade_ids"] == []
    assert order["model_diagnostics"]["selected_route"] == (
        "taker_hierarchical_expected"
    )
    assert order["model_diagnostics"]["prior_only_used"] is False
    assert order["model_diagnostics"]["expected_fill_is_observed_execution"] is False
    assert order["model_diagnostics"]["hierarchical_context"]["league"] == "esports"
    assert result["summary"]["modeled_expectation_orders"] == 1
    assert result["summary"]["positive_modeled_expectation_orders"] == 1


def test_v3_service_accepts_real_scale_market_and_block_ids() -> None:
    payload = _payload()
    payload["orders"][0]["marketId"] = 2_416_400
    payload["orders"][0]["signalBlock"] = 89_502_028
    payload["sourceMaxBlock"] = 90_792_478

    clickhouse = FakeClickHouse()
    clickhouse.query_json_rows = lambda query, timeout_seconds=None: (
        [{"from_block": 82_391_997, "to_block": 90_792_478, "build_tag": "real-scale"}]
        if "orderfilled_v2_build_chunks" in query
        else []
    )

    result = run_fill_only_v3_replay(payload, client=clickhouse)

    assert result["manifest"]["orders"][0]["market_id"] == 2_416_400
    assert result["manifest"]["orders"][0]["signal_block"] == 89_502_028


def test_v3_http_routes(monkeypatch) -> None:
    monkeypatch.setattr(
        quant_routes,
        "build_fill_only_v3_readiness",
        lambda: {"ready": True, "uses_lob_data": False},
    )
    monkeypatch.setattr(
        quant_routes,
        "run_fill_only_v3_replay",
        lambda payload: {"request_id": payload["requestId"], "uses_lob_data": False},
    )
    monkeypatch.setattr(
        quant_routes,
        "resolve_fill_only_v3_anchor",
        lambda payload: {"request_id": payload["requestId"], "anchor_block": 100},
    )
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))
    client = app.test_client()

    profiles = client.get("/quant/fill-only/v3/profiles")
    readiness = client.get("/quant/fill-only/v3/readiness")
    replay = client.post("/quant/fill-only/v3/replay", json={"requestId": "v3"})
    anchor = client.post(
        "/quant/fill-only/v3/resolve-anchor", json={"requestId": "anchor"}
    )

    assert profiles.status_code == 200
    assert (
        profiles.get_json()["default_profile"]
        == "central_trade_only_l2_reference_expected_fak"
    )
    assert readiness.status_code == 200
    assert replay.status_code == 200
    assert replay.get_json()["request_id"] == "v3"
    assert anchor.status_code == 200
    assert anchor.get_json()["anchor_block"] == 100


class AnchorClickHouse(FakeClickHouse):
    def query_json_rows(
        self, query: str, *, timeout_seconds: float | None = None
    ) -> list[dict]:
        self.queries.append(query)
        if "orderfilled_v2_build_chunks" in query:
            return [{"from_block": 1, "to_block": 1_000, "build_tag": "anchor"}]
        if "'before' AS relation" in query:
            return [
                {
                    "relation": "before",
                    "trade_id": "before",
                    "block_number": 99,
                    "block_time": "2026-06-01T11:59:59Z",
                },
                {
                    "relation": "after",
                    "trade_id": "after",
                    "block_number": 101,
                    "block_time": "2026-06-01T12:00:01Z",
                },
            ]
        if "trade_prints_one_sided" in query:
            return list(self.trades)
        raise AssertionError(f"unexpected query: {query}")


def test_v3_resolve_anchor_is_strict_and_orderfilled_only() -> None:
    client = AnchorClickHouse()

    resolved = resolve_fill_only_v3_anchor(
        {"signalTs": "2026-06-01T12:00:00Z", "maxDistanceSeconds": 5},
        client=client,
    )

    assert resolved["anchor_block"] == 100
    assert resolved["uses_lob_data"] is False
    assert resolved["schema_version"] == "fill-only-v3-timestamp-anchor-v1"
    assert all("lob" not in query.lower() for query in client.queries)
    with pytest.raises(FillOnlyV3RequestError, match="unknown fields"):
        resolve_fill_only_v3_anchor(
            {"signalTs": "2026-06-01T12:00:00Z", "typo": 1}, client=client
        )


def test_v3_timestamp_native_replay_resolves_missing_signal_block() -> None:
    client = AnchorClickHouse(
        [
            _trade(
                "pre-buy",
                block=98,
                block_time="2026-06-01T11:59:57Z",
                side="BUY",
                price="0.55",
            ),
            _trade(
                "pre-sell",
                block=99,
                block_time="2026-06-01T11:59:58Z",
                side="SELL",
                price="0.53",
            ),
        ]
    )
    payload = _payload()
    payload["orders"][0].pop("signalBlock")

    result = run_fill_only_v3_replay(payload, client=client)

    assert result["manifest"]["orders"][0]["signal_block"] == 100
    assert result["timestamp_anchors"][0]["anchor_block"] == 100
