from decimal import Decimal

import quant.backtest.execution_profile_overrides as overrides
from quant.backtest.execution_profile_overrides import (
    apply_execution_profile_override_to_payload,
    maybe_apply_approved_execution_profile_override,
    normalize_execution_profile_override,
    select_approved_execution_profile_override,
)


def test_normalize_execution_profile_override_from_suggestion() -> None:
    suggestion = {
        "scope": "bucket",
        "bucket_field": "liquidity_bucket",
        "bucket": "thin",
        "sample_count": 12,
        "trust_reason": "status error 25%",
        "recommended": {
            "execution_profile": "stress",
            "latency_blocks_floor": 1,
            "adverse_slippage_price_floor": "0.035",
            "fill_probability_haircut_pct_floor": "37.5",
        },
        "evidence": {"status_error_rate": "25"},
    }

    override = normalize_execution_profile_override(
        suggestion,
        status="approved",
        source="unit-test",
        approved_by="reviewer",
    )

    assert override["status"] == "approved"
    assert override["scope"] == "bucket"
    assert override["bucket_field"] == "liquidity_bucket"
    assert override["bucket_value"] == "thin"
    assert override["execution_profile"] == "stress"
    assert override["latency_blocks"] == 1
    assert override["adverse_slippage_cents"] == Decimal("0.035")
    assert override["fill_probability_haircut_pct"] == Decimal("37.5")
    assert override["approved"] is True
    assert override["approved_by"] == "reviewer"


def test_apply_execution_profile_override_keeps_more_conservative_user_values() -> None:
    payload = {
        "executionProfile": "realistic",
        "latencyBlocks": 3,
        "adverseSlippageCents": "0.05",
        "fillProbabilityHaircutPct": "60",
        "executionContext": {"caller": "test"},
    }
    override = {
        "override_id": 9,
        "status": "approved",
        "source": "calibration",
        "scope": "overall",
        "execution_profile": "conservative",
        "latency_blocks": 1,
        "adverse_slippage_cents": Decimal("0.02"),
        "fill_probability_haircut_pct": Decimal("30"),
        "calibration_sample_count": 20,
    }

    applied = apply_execution_profile_override_to_payload(payload, override)

    assert applied["execution_profile"] == "conservative"
    assert applied["latency_blocks"] == 3
    assert applied["adverse_slippage_cents"] == Decimal("0.05")
    assert applied["fill_probability_haircut_pct"] == Decimal("60")
    assert applied["execution_context"]["caller"] == "test"
    assert applied["execution_context"]["execution_profile_override"]["override_id"] == 9


def test_apply_execution_profile_override_sets_missing_values() -> None:
    payload = {"market_slug": "x"}
    override = {
        "override_id": 10,
        "status": "approved",
        "source": "calibration",
        "scope": "overall",
        "execution_profile": "stress",
        "latency_blocks": 2,
        "adverse_slippage_cents": Decimal("0.04"),
        "fill_probability_haircut_pct": Decimal("75"),
        "calibration_sample_count": 40,
    }

    applied = apply_execution_profile_override_to_payload(payload, override)

    assert applied["execution_profile"] == "stress"
    assert applied["latency_blocks"] == 2
    assert applied["adverse_slippage_cents"] == Decimal("0.04")
    assert applied["fill_probability_haircut_pct"] == Decimal("75")


def test_select_approved_override_prefers_explicit_id(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_load(conn, **kwargs):
        calls.append(kwargs)
        if kwargs.get("override_id") == 42:
            return {"override_id": 42, "scope": "overall", "status": "approved", "execution_profile": "conservative"}
        if kwargs.get("scope") == "bucket":
            return {"override_id": 99, "scope": "bucket", "status": "approved", "execution_profile": "stress"}
        return None

    monkeypatch.setattr(overrides, "load_execution_profile_override", fake_load)

    selected = select_approved_execution_profile_override(
        object(),
        {
            "executionProfileOverrideId": 42,
            "useCalibratedExecutionProfile": True,
            "executionContext": {"liquidityBucket": "thin"},
        },
    )

    assert selected["override_id"] == 42
    assert calls == [{"override_id": 42, "status": "approved"}]


def test_select_approved_override_prefers_bucket_before_overall(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_load(conn, **kwargs):
        calls.append(kwargs)
        if kwargs.get("scope") == "bucket" and kwargs.get("bucket_field") == "liquidity_bucket" and kwargs.get("bucket_value") == "thin":
            return {
                "override_id": 7,
                "scope": "bucket",
                "bucket_field": "liquidity_bucket",
                "bucket_value": "thin",
                "status": "approved",
                "execution_profile": "stress",
                "latency_blocks": 2,
                "adverse_slippage_cents": Decimal("0.04"),
                "fill_probability_haircut_pct": Decimal("70"),
            }
        if kwargs.get("scope") == "overall":
            return {"override_id": 1, "scope": "overall", "status": "approved", "execution_profile": "conservative"}
        return None

    monkeypatch.setattr(overrides, "load_execution_profile_override", fake_load)

    applied = maybe_apply_approved_execution_profile_override(
        object(),
        {
            "use_calibrated_execution_profile": True,
            "execution_context": {"liquidity_bucket": "thin", "market_category": "sports"},
        },
    )

    assert applied["execution_profile"] == "stress"
    assert applied["latency_blocks"] == 2
    assert applied["execution_context"]["execution_profile_override"]["override_id"] == 7
    assert applied["execution_context"]["execution_profile_override"]["bucket_field"] == "liquidity_bucket"
    assert any(call.get("scope") == "bucket" for call in calls)
    assert not any(call.get("scope") == "overall" for call in calls)


def test_select_approved_override_falls_back_to_overall(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_load(conn, **kwargs):
        calls.append(kwargs)
        if kwargs.get("scope") == "overall":
            return {
                "override_id": 3,
                "scope": "overall",
                "status": "approved",
                "execution_profile": "conservative",
                "latency_blocks": 1,
                "adverse_slippage_cents": Decimal("0.02"),
                "fill_probability_haircut_pct": Decimal("25"),
            }
        return None

    monkeypatch.setattr(overrides, "load_execution_profile_override", fake_load)

    applied = maybe_apply_approved_execution_profile_override(
        object(),
        {
            "useCalibratedExecutionProfile": True,
            "executionContext": {"timeToExpiryBucket": "lt_7d"},
        },
    )

    assert applied["execution_profile"] == "conservative"
    assert applied["execution_context"]["execution_profile_override"]["override_id"] == 3
    assert any(call.get("scope") == "bucket" and call.get("bucket_field") == "time_to_expiry_bucket" for call in calls)
    assert calls[-1]["scope"] == "overall"
