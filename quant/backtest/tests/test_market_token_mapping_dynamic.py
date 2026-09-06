from __future__ import annotations

from decimal import Decimal

from quant.market.market_token_mapping import normalize_market_payload


def test_normalize_clob_token_ids_json_string() -> None:
    _, tokens, warnings = normalize_market_payload(
        {
            "id": "m1",
            "conditionId": "0xcond",
            "clobTokenIds": '["111","222"]',
            "outcomes": ["YES", "NO"],
        },
        "gamma",
    )

    assert warnings == []
    assert [token.asset_id for token in tokens] == ["111", "222"]


def test_normalize_clob_token_ids_array() -> None:
    _, tokens, _ = normalize_market_payload(
        {"id": "m1", "conditionId": "0xcond", "clobTokenIds": ["111", "222"], "outcomes": ["UP", "DOWN"]},
        "gamma",
    )

    assert [token.outcome_name for token in tokens] == ["UP", "DOWN"]


def test_normalize_outcomes_json_string() -> None:
    _, tokens, _ = normalize_market_payload(
        {"id": "m1", "conditionId": "0xcond", "clobTokenIds": ["111", "222"], "outcomes": '["A","B"]'},
        "gamma",
    )

    assert [token.outcome_name for token in tokens] == ["A", "B"]


def test_normalize_multi_outcome_market() -> None:
    _, tokens, _ = normalize_market_payload(
        {
            "id": "m1",
            "conditionId": "0xcond",
            "clobTokenIds": ["111", "222", "333"],
            "outcomes": ["A", "B", "C"],
        },
        "gamma",
    )

    assert [token.outcome_index for token in tokens] == [0, 1, 2]
    assert [token.is_yes for token in tokens] == [False, False, False]
    assert [token.is_no for token in tokens] == [False, False, False]


def test_low_confidence_when_token_outcome_count_mismatch() -> None:
    _, tokens, warnings = normalize_market_payload(
        {"id": "m1", "conditionId": "0xcond", "clobTokenIds": ["111", "222"], "outcomes": ["YES"]},
        "gamma",
    )

    assert "token_outcome_count_mismatch" in warnings
    assert len(tokens) == 2
    assert tokens[1].outcome_name is None


def test_decimal_no_float_conversion() -> None:
    market, tokens, _ = normalize_market_payload(
        {
            "id": "m1",
            "conditionId": "0xcond",
            "clobTokenIds": ["111"],
            "outcomes": ["YES"],
            "tickSize": 0.01,
            "minOrderSize": "5",
        },
        "gamma",
    )

    assert market.current_tick_size == Decimal("0.01")
    assert market.min_order_size == Decimal("5")
    assert tokens[0].current_tick_size == Decimal("0.01")
