from __future__ import annotations

import json
import math
import random
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from quant.backtest.orderfilled_probability import (
    HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED,
    OrderFilledProbabilityModel,
    augment_orderfilled_probability_vector,
    default_orderfilled_probability_profile,
    extract_orderfilled_probability_features,
    future_same_side_fill_label,
    hierarchical_probability_keys,
)
from quant.backtest.orderfilled_v2_replay import (
    V2TakerOrder,
    V2TradePrint,
    build_required_trade_windows,
    replay_v2_taker_order,
    with_v2_execution_profile,
)

BASE_TIME = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _trade(
    trade_id: str, block: int, side: str, price: str = "0.50", size: str = "100"
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=1,
        condition_id="condition",
        asset_id="token",
        outcome="YES",
        block_number=block,
        block_time=BASE_TIME + timedelta(seconds=block - 100),
        tx_hash=f"0x{trade_id}",
        tx_index=0,
        tx_index_source="fixture",
        price=Decimal(price),
        size=Decimal(size),
        notional=Decimal(price) * Decimal(size),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(block,),
        source_fill_count=1,
    )


def _order() -> V2TakerOrder:
    return V2TakerOrder(
        order_id="probability-order",
        market_id=1,
        asset_id="token",
        side="BUY",
        limit_price=Decimal("0.53"),
        size=Decimal(10),
        signal_block=99,
        signal_ts=BASE_TIME - timedelta(seconds=1),
        latency_blocks=1,
        latency=timedelta(seconds=1),
        horizon_blocks=10,
        horizon=timedelta(seconds=10),
        participation_rate=Decimal("0.10"),
    )


def _metadata_order(
    *,
    category: str,
    market_slug: str = "",
    market_title: str = "",
    limit_price: str = "0.03",
) -> SimpleNamespace:
    base = _order()
    fields = {**base.__dict__, "limit_price": Decimal(limit_price)}
    return SimpleNamespace(
        **fields,
        arrival_ts=base.arrival_ts,
        arrival_block=base.arrival_block,
        deadline_ts=base.deadline_ts,
        deadline_block=base.deadline_block,
        category=category,
        market_slug=market_slug,
        market_title=market_title,
        league=None,
    )


def _profile(
    *, intercept: str, min_probability: str = "0.50", hard_reject: bool = False
) -> dict:
    row = default_orderfilled_probability_profile().as_dict()
    row["intercept"] = intercept
    row["min_probability"] = min_probability
    row["medium_probability"] = min_probability
    row["capacity_floor"] = "1"
    row["hard_reject_below_probability"] = hard_reject
    row["coefficients"] = {name: "0" for name in row["coefficients"]}
    return row


def _two_stage_profile(
    *, probability_intercept: str, capacity: str, variant: str = "expected"
) -> dict:
    row = _profile(intercept=probability_intercept)
    row["capacity_mode"] = "conditional_source_capacity"
    row["capacity_variant"] = variant
    row["conditional_capacity"] = {
        "mode": "conditional_source_capacity",
        "default_variant": variant,
        "models": {
            variant: {
                "name": variant,
                "target": "conditional_fill_fraction",
                "quantile": None,
                "floor": "0",
                "intercept": capacity,
                "coefficients": {name: "0" for name in row["coefficients"]},
                "training_rows": 100,
            }
        },
    }
    return row


def test_features_use_only_pre_arrival_orderfilled_rows() -> None:
    order = _order()
    before = [_trade("same", 99, "BUY"), _trade("opposite", 98, "SELL")]
    future = _trade("future", 101, "BUY", size="100000")

    without_future = extract_orderfilled_probability_features(order, before)
    with_future = extract_orderfilled_probability_features(order, [*before, future])

    assert with_future.vector == without_future.vector
    assert with_future.trailing_same_count == 1
    assert with_future.trailing_opposite_count == 1
    assert with_future.vector["log_order_size"] == Decimal(
        str(math.log1p(float(order.size)))
    ).quantize(Decimal("0.0000000001"))


def test_nonlinear_features_are_deterministic_at_bucket_boundaries() -> None:
    vector = augment_orderfilled_probability_vector(
        {
            "limit_aggressiveness_ticks": "6",
            "limit_price": "0.97",
            "log_total_trade_count": str(math.log1p(6)),
            "same_flow_share": "0.80",
        },
        last_same_age_seconds="20",
        last_opposite_age_seconds="200",
        order_size="100",
    )

    assert vector["aggressiveness_nonnegative"] == 1
    assert vector["aggressiveness_ge_2"] == 1
    assert vector["aggressiveness_ge_5"] == 1
    assert vector["aggressiveness_ge_10"] == 0
    assert vector["any_recent_30s"] == 1
    assert vector["any_recent_120s"] == 1
    assert vector["price_bucket_95_100"] == 1
    assert vector["trades_ge_6"] == 1
    assert vector["trades_ge_20"] == 0
    assert vector["trades_ge_50"] == 0
    assert vector["flow_imbalance_abs"] == Decimal("0.60")
    assert vector["log_order_size"] == Decimal(str(math.log1p(100)))


def test_probability_serialization_matches_legacy_recursive_asdict() -> None:
    decision = OrderFilledProbabilityModel().decide(
        _order(),
        [_trade("same", 99, "BUY"), _trade("opposite", 98, "SELL")],
    )
    legacy = json.loads(json.dumps(asdict(decision), default=str))

    assert decision.as_dict() == legacy


def test_probability_is_not_hard_zero_when_pre_arrival_same_side_trade_is_missing() -> (
    None
):
    order = V2TakerOrder(
        **{
            **_order().__dict__,
            "orderfilled_probability_profile": _profile(intercept="2"),
        }
    )
    decision = OrderFilledProbabilityModel(
        order.orderfilled_probability_profile
    ).decide(
        order,
        [_trade("opposite", 99, "SELL")],
    )

    assert decision.accepted is True
    assert decision.p_fill > Decimal("0.80")
    assert decision.features.trailing_same_count == 0


def test_probability_uses_category_family_calibration_when_available() -> None:
    profile = _profile(intercept="0")
    profile["calibration"] = {
        "x": ["0", "1"],
        "y": ["0.2", "0.2"],
        "by_category_family": {"crypto": {"x": ["0", "1"], "y": ["0.8", "0.8"]}},
    }
    model = OrderFilledProbabilityModel(profile)

    crypto = model.decide(_metadata_order(category="crypto"), [])
    sports = model.decide(_metadata_order(category="sports"), [])

    assert crypto.base_probability == Decimal("0.8000000000")
    assert sports.base_probability == Decimal("0.2000000000")


def test_probability_classifies_common_sports_leagues_as_sports() -> None:
    profile = _profile(intercept="0")
    profile["calibration"] = {
        "x": ["0", "1"],
        "y": ["0.2", "0.2"],
        "by_category_family": {"sports": {"x": ["0", "1"], "y": ["0.8", "0.8"]}},
    }
    model = OrderFilledProbabilityModel(profile)

    for category in ("atp-tour", "wta", "cfb", "ncaa-basketball", "pga"):
        decision = model.decide(_metadata_order(category=category), [])
        assert decision.base_probability == Decimal("0.8000000000")


def test_probability_abstains_from_unstable_activity_regime() -> None:
    profile = _profile(intercept="2")
    profile["domain_gate"] = {"abstain_activity_regimes": ["ACTIVE_GT_50"]}
    model = OrderFilledProbabilityModel(profile)
    trades = [_trade(f"active-{index}", 48 + index, "BUY") for index in range(51)]

    decision = model.decide(_metadata_order(category="sports"), trades)

    assert decision.domain_supported is False
    assert decision.accepted is False
    assert decision.reason == "orderfilled_probability_model_out_of_domain"


def test_low_probability_rejects_even_when_future_trade_exists() -> None:
    order = V2TakerOrder(
        **{
            **_order().__dict__,
            "orderfilled_probability_profile": _profile(
                intercept="-10", hard_reject=True
            ),
        }
    )
    result = replay_v2_taker_order(order, [_trade("future", 101, "BUY", "0.52")])

    assert result.status == "NO_FILL"
    assert result.reason_unfilled == "low_orderfilled_fill_probability"
    assert result.p_fill is not None and result.p_fill < Decimal("0.01")


def test_accepted_probability_still_requires_real_same_side_source_trade() -> None:
    order = V2TakerOrder(
        **{
            **_order().__dict__,
            "orderfilled_probability_profile": _profile(intercept="10"),
        }
    )
    no_source = replay_v2_taker_order(order, [_trade("opposite", 101, "SELL")])
    observed = replay_v2_taker_order(
        order, [_trade("opposite", 99, "SELL"), _trade("source", 101, "BUY", "0.52")]
    )

    assert no_source.status == "NO_FILL"
    assert no_source.reason_unfilled == "no_post_arrival_same_side_trade"
    assert observed.status == "FILLED"
    assert observed.fills[0].source_trade_id == "source"
    assert observed.fills[0].source_tx_hash == "0xsource"
    assert observed.p_fill is not None and observed.p_fill > Decimal("0.99")


def test_two_stage_capacity_does_not_discount_real_source_by_arrival_probability_twice() -> (
    None
):
    order = V2TakerOrder(
        **{
            **_order().__dict__,
            "orderfilled_probability_profile": _two_stage_profile(
                probability_intercept="-10",
                capacity="1",
            ),
        }
    )
    result = replay_v2_taker_order(order, [_trade("source", 101, "BUY", "0.52", "100")])

    assert result.p_fill is not None and result.p_fill < Decimal("0.01")
    assert result.conditional_capacity_fraction == Decimal("1.0000000000")
    assert result.fill_capacity_variant == "expected"
    assert result.status == "FILLED"
    assert result.filled_size == order.size


def test_two_stage_expected_and_conservative_variants_only_change_order_capacity() -> (
    None
):
    expected_order = V2TakerOrder(
        **{
            **_order().__dict__,
            "order_id": "expected",
            "orderfilled_probability_profile": _two_stage_profile(
                probability_intercept="0",
                capacity="0.80",
                variant="expected",
            ),
        }
    )
    conservative_order = V2TakerOrder(
        **{
            **_order().__dict__,
            "order_id": "conservative",
            "orderfilled_probability_profile": _two_stage_profile(
                probability_intercept="0",
                capacity="0.25",
                variant="conservative",
            ),
        }
    )
    source = _trade("source", 101, "BUY", "0.52", "100")
    expected = replay_v2_taker_order(expected_order, [source])
    conservative = replay_v2_taker_order(conservative_order, [source])

    assert expected.filled_size == Decimal("8.0000000000")
    assert conservative.filled_size == Decimal("2.5000000000")
    assert (
        expected.fills[0].source_trade_id
        == conservative.fills[0].source_trade_id
        == "source"
    )


def test_probability_profile_loads_pre_arrival_both_side_window() -> None:
    order = with_v2_execution_profile(_order(), "probabilistic_trade_tape")
    windows = build_required_trade_windows([order])

    assert len(windows) == 1
    assert windows[0].aggressor_side is None
    assert windows[0].start_block < order.arrival_block
    assert windows[0].end_block == order.deadline_block


def test_orderfilled_label_respects_side_limit_horizon() -> None:
    order = _order()
    label, volume = future_same_side_fill_label(
        order,
        [
            _trade("wrong-side", 101, "SELL", "0.50", "100"),
            _trade("outside-limit", 102, "BUY", "0.55", "100"),
            _trade("eligible", 103, "BUY", "0.52", "25"),
            _trade("late", 120, "BUY", "0.52", "100"),
        ],
        price_buffer=Decimal("0.005"),
    )

    assert label == 1
    assert volume == Decimal("25.0000000000")


def test_probability_runtime_module_has_no_external_market_data_dependency() -> None:
    source = (
        Path("quant/backtest/orderfilled_probability.py")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "orderbook" not in source
    assert "fill_only_lob_validity" not in source


def test_hierarchical_probability_prefers_raw_category_price_cell() -> None:
    profile = _profile(intercept="10")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "cells": {
            "category_price|democratic-national-committee|00_05": {
                "probability": "0.10",
                "samples": 20,
            },
            "family_price|politics|00_05": {
                "probability": "0.80",
                "samples": 100,
            },
            "global": {"probability": "0.60", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(category="democratic-national-committee"),
        [_trade("same", 99, "BUY")],
    )

    assert decision.base_probability > Decimal("0.99")
    assert decision.p_fill == Decimal("0.1000000000")
    assert (
        decision.hierarchical_probability_cell
        == "category_price|democratic-national-committee|00_05"
    )
    assert decision.hierarchical_probability_samples == 20


def test_hierarchical_probability_applies_artifact_scale() -> None:
    profile = _profile(intercept="-10")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "probability_scale": "1.10",
        "cells": {
            "family_price|crypto|00_05": {
                "probability": "0.40",
                "samples": 30,
            },
            "global": {"probability": "0.40", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(
            category="crypto",
            market_slug="will-bitcoin-reach-a-new-high",
        ),
        [_trade("same", 99, "BUY")],
    )

    assert decision.p_fill == Decimal("0.4400000000")


def test_hierarchical_probability_falls_back_to_family_price_cell() -> None:
    profile = _profile(intercept="-10")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "cells": {
            "family_price|crypto|00_05": {
                "probability": "0.70",
                "samples": 30,
            },
            "global": {"probability": "0.40", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(
            category="new-unseen-category",
            market_slug="will-bitcoin-reach-a-new-high",
            market_title="Will Bitcoin reach a new high?",
        ),
        [_trade("same", 99, "BUY")],
    )

    assert decision.p_fill == Decimal("0.7000000000")
    assert decision.hierarchical_probability_cell == "family_price|crypto|00_05"


def test_failed_hierarchy_category_falls_back_to_calibrated_base() -> None:
    profile = _profile(intercept="0")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "fallback_categories": ["democratic-national-committee"],
        "cells": {
            "category_price|democratic-national-committee|00_05": {
                "probability": "0.10",
                "samples": 100,
            },
            "global": {"probability": "0.90", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(category="democratic-national-committee"),
        [_trade("same", 99, "BUY")],
    )

    assert decision.domain_supported is True
    assert decision.p_fill == decision.base_probability
    assert decision.hierarchical_probability is None
    assert decision.hierarchical_probability_cell is None


def test_failed_category_skips_category_cell_but_keeps_family_evidence() -> None:
    profile = _profile(intercept="-10")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "fallback_categories": ["democratic-national-committee"],
        "cells": {
            "category_price|democratic-national-committee|00_05": {
                "probability": "0.10",
                "samples": 100,
            },
            "family_price|politics|00_05": {
                "probability": "0.70",
                "samples": 100,
            },
            "global": {"probability": "0.90", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(category="democratic-national-committee"),
        [_trade("same", 99, "BUY")],
    )

    assert decision.p_fill == Decimal("0.7000000000")
    assert decision.hierarchical_probability_cell == "family_price|politics|00_05"


def test_hierarchical_keys_use_only_order_metadata_and_pre_arrival_tape() -> None:
    order = _metadata_order(category="crypto", limit_price="0.51")
    features = extract_orderfilled_probability_features(
        order,
        [_trade("same", 99, "BUY"), _trade("opposite", 98, "SELL")],
    )

    keys = hierarchical_probability_keys(order, features.vector)

    assert keys[0] == ("market_asset_price_activity_hour|1|token|50_80|sparse|20_24")
    assert "category_price_activity_exact_hour|crypto|50_80|sparse|23" in keys
    assert "category_price_activity_hour|crypto|50_80|sparse|20_24" in keys
    assert "family_price_activity|crypto|50_80|sparse" in keys
    assert keys[-1] == "global"


def test_hierarchical_keys_prefer_category_backoff_over_family_detail() -> None:
    keys = hierarchical_probability_keys(
        _metadata_order(category="tech", limit_price="0.15")
    )

    assert "family_price_activity_hour|other|05_20|unknown|20_24" not in keys
    assert keys.index("category|tech") < keys.index("price|05_20")


def test_validation_aligned_hierarchy_keys_match_quality_buckets() -> None:
    order = _metadata_order(category="crypto", limit_price="0.82")
    features = extract_orderfilled_probability_features(
        order,
        [_trade(str(index), 99 - index, "BUY") for index in range(8)],
    )

    keys = hierarchical_probability_keys(
        order,
        features.vector,
        key_scheme=HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED,
    )

    assert "contract_price_activity|buy|gtc|le_10|75_90|medium" in keys
    assert "category_price_activity|crypto|75_90|medium" in keys


def test_hierarchy_skips_category_without_independent_support() -> None:
    profile = _profile(intercept="0")
    profile["hierarchical_probability"] = {
        "minimum_samples": 20,
        "blend_strength": "0",
        "supported_categories": ["tech"],
        "cells": {
            "category|science": {"probability": "0.90", "samples": 100},
            "global": {"probability": "0.40", "samples": 1000},
        },
    }

    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(category="science"),
        [_trade("same", 99, "BUY")],
    )

    assert decision.p_fill == decision.base_probability
    assert decision.hierarchical_probability_cell is None


def test_profile_without_hierarchy_preserves_base_probability() -> None:
    profile = _profile(intercept="0")
    decision = OrderFilledProbabilityModel(profile).decide(
        _metadata_order(category="democratic-national-committee"),
        [_trade("same", 99, "BUY")],
    )

    assert decision.p_fill == decision.base_probability
    assert decision.hierarchical_probability is None
    assert decision.hierarchical_probability_cell is None
    assert decision.hierarchical_probability_samples == 0


def test_randomized_probability_fills_always_keep_orderfilled_source_invariants() -> (
    None
):
    rng = random.Random(20260723)
    for case in range(100):
        side = rng.choice(["BUY", "SELL"])
        limit = Decimal("0.55") if side == "BUY" else Decimal("0.45")
        order = V2TakerOrder(
            **{
                **_order().__dict__,
                "order_id": f"random-{case}",
                "side": side,
                "limit_price": limit,
                "size": Decimal(str(rng.randint(1, 20))),
                "participation_rate": Decimal("0.05"),
                "orderfilled_probability_profile": _profile(intercept="10"),
            }
        )
        rows = [
            _trade(
                f"random-{case}-{index}",
                101 + index,
                rng.choice(["BUY", "SELL"]),
                str(Decimal(rng.randint(40, 60)) / Decimal(100)),
                str(rng.randint(1, 200)),
            )
            for index in range(5)
        ]
        result = replay_v2_taker_order(order, rows)
        by_id = {row.trade_id: row for row in rows}
        assert result.filled_size <= order.size
        for fill in result.fills:
            source = by_id[fill.source_trade_id]
            assert source.aggressor_side == side
            assert fill.source_tx_hash == source.tx_hash
            assert fill.filled_size <= source.size * order.participation_rate
            assert (
                fill.exec_price <= order.limit_price
                if side == "BUY"
                else fill.exec_price >= order.limit_price
            )
