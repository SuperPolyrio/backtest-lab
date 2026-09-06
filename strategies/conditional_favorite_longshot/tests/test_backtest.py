from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant.backtest.orderfilled_v2_replay import V2TradePrint, replay_v2_taker_order
from quant.backtest.runners.selectors import ResolvedMarketCandidate
from strategies.conditional_favorite_longshot.backtest import (
    BacktestConfig,
    Candidate,
    _economic_trade_rows,
    _favorite_at_snapshot,
    build_candidates,
    build_walk_forward_signals,
    load_orderfilled_tape,
    signal_to_order,
)


NOW = datetime(2025, 1, 2, 1, tzinfo=timezone.utc)


def _trade(
    trade_id: str,
    *,
    market_id: int = 1,
    outcome: str,
    price: str,
    minutes: int,
    side: str = "BUY",
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=market_id,
        condition_id="condition",
        asset_id=f"token-{outcome.lower()}",
        outcome=outcome,
        block_number=1000 + minutes,
        block_time=NOW + timedelta(minutes=minutes),
        tx_hash=f"0x{trade_id}",
        tx_index=0,
        tx_index_source="fixture",
        price=Decimal(price),
        size=Decimal("100"),
        notional=Decimal(price) * Decimal("100"),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(minutes + 100,),
        source_fill_count=1,
    )


def _candidate(index: int, *, won: bool, price_bucket: str = "0.65-0.70") -> Candidate:
    event = NOW + timedelta(days=index)
    return Candidate(
        market_id=index,
        market_slug=f"nba-{index}",
        title=f"NBA {index}",
        event_time=event,
        signal_time=event - timedelta(hours=1),
        label_available_at=event + timedelta(hours=6),
        settlement_code=1 if won else 2,
        asset_id="token-yes",
        outcome_code=1,
        favorite_side="YES",
        signal_price=Decimal("0.65"),
        signal_trade_id=f"signal-{index}",
        signal_tx_hash=f"0x{index}",
        signal_log_indexes=(index,),
        signal_block=1000 + index,
        trailing_trade_count=100,
        trailing_volume=Decimal("1000"),
        price_bucket=price_bucket,
        liquidity_bucket="50-199",
        won=won,
    )


def test_favorite_selection_can_buy_no() -> None:
    favorite = _favorite_at_snapshot(
        [
            _trade("yes", outcome="YES", price="0.35", minutes=-70),
            _trade("no", outcome="NO", price="0.65", minutes=-70),
        ]
    )

    assert favorite is not None
    assert favorite[0].outcome == "NO"
    assert favorite[1] == 2


def test_walk_forward_uses_only_labels_available_before_signal() -> None:
    history = [_candidate(index, won=True) for index in range(1, 22)]
    current = _candidate(22, won=False)
    config = BacktestConfig(
        min_samples=5,
        prior_strength=Decimal("1"),
        edge_threshold=Decimal("-1"),
    )

    signals, skipped_samples, _ = build_walk_forward_signals([*history, current], config=config)
    current_signal = next(row for row in signals if row.candidate.market_id == current.market_id)

    assert current_signal.calibration_samples == 21
    assert current_signal.calibration_wins == 21
    assert skipped_samples > 0


def test_candidate_and_v2_execution_require_post_signal_source() -> None:
    market = ResolvedMarketCandidate(
        market_id=1,
        market_slug="nba-a-b-2025-01-02",
        title="A vs B",
        end_date=NOW + timedelta(hours=1),
        settlement_code=2,
        settlement_outcome="NO",
    )
    history = []
    for index in range(20):
        history.extend(
            [
                _trade(f"yes-{index}", outcome="YES", price="0.35", minutes=-120 + index, side="BUY"),
                _trade(f"no-{index}", outcome="NO", price="0.65", minutes=-120 + index, side="BUY"),
            ]
        )
    candidates = build_candidates([market], history, config=BacktestConfig())
    assert len(candidates) == 1
    assert candidates[0].favorite_side == "NO"

    signal = type(
        "SignalLike",
        (),
        {
            "candidate": candidates[0],
            "posterior_win_rate": Decimal("0.75"),
            "net_edge": Decimal("0.08"),
        },
    )()
    order = signal_to_order(signal, config=BacktestConfig(), profile="probabilistic_source_confirmed")
    no_source = replay_v2_taker_order(order, history)
    source = _trade("post-source", outcome="NO", price="0.65", minutes=1, side="BUY")
    filled = replay_v2_taker_order(order, [*history, source])

    assert no_source.status == "NO_FILL"
    assert order.deadline_ts == order.arrival_ts + timedelta(minutes=5)
    assert filled.filled_size > 0
    assert filled.fills[0].source_trade_id == "post-source"


def test_legacy_tape_loader_uses_clean_economic_price() -> None:
    class Client:
        query = ""

        def query_json_rows(self, query: str, **_: object) -> list[dict[str, object]]:
            self.query = query
            return []

    market = ResolvedMarketCandidate(
        market_id=1,
        market_slug="nba-a-b-2025-01-02",
        title="A vs B",
        end_date=NOW,
        settlement_code=1,
        settlement_outcome="YES",
    )
    client = Client()

    assert load_orderfilled_tape([market], config=BacktestConfig(), client=client) == []  # type: ignore[arg-type]
    assert "least(assumeNotNull(f.maker_amount)" in client.query
    assert "c5d563a36ae78145c45a50134d48a1215220f80a" in client.query


def test_candidate_uses_slug_date_when_end_date_is_shifted() -> None:
    market = ResolvedMarketCandidate(
        market_id=1,
        market_slug="nba-a-b-2025-01-02",
        title="A vs B",
        end_date=datetime(2025, 1, 10, 1, tzinfo=timezone.utc),
        settlement_code=1,
        settlement_outcome="YES",
    )
    trades = []
    for index in range(20):
        trades.extend(
            [
                _trade(f"shifted-yes-{index}", outcome="YES", price="0.65", minutes=-120 + index),
                _trade(f"shifted-no-{index}", outcome="NO", price="0.35", minutes=-120 + index),
            ]
        )

    candidate = build_candidates([market], trades, config=BacktestConfig(min_trades=20))[0]

    assert candidate.event_time == NOW


def test_signal_price_uses_latest_trade_and_implied_complement() -> None:
    market = ResolvedMarketCandidate(
        market_id=1,
        market_slug="nba-a-b-2025-01-02",
        title="A vs B",
        end_date=NOW,
        settlement_code=2,
        settlement_outcome="NO",
    )
    trades = [
        _trade("old-yes", outcome="YES", price="0.62", minutes=-100),
        _trade("new-no", outcome="NO", price="0.55", minutes=-61),
    ]

    candidate = build_candidates(
        [market],
        trades,
        config=BacktestConfig(min_trades=1, min_probability=Decimal("0.50")),
    )[0]

    assert candidate.favorite_side == "NO"
    assert candidate.signal_price == Decimal("0.5500000000")
    assert candidate.signal_trade_id == "new-no"
    assert candidate.pair_sum_error == Decimal("0.17")


def test_zero_residual_prior_does_not_manufacture_global_favorite_edge() -> None:
    history = []
    for index in range(1, 21):
        base = _candidate(index, won=index <= 12)
        history.append(replace(base, signal_price=Decimal("0.60"), price_bucket="0.60-0.65"))
    current = replace(
        _candidate(30, won=True),
        signal_price=Decimal("0.60"),
        price_bucket="0.60-0.65",
    )
    config = BacktestConfig(
        min_samples=20,
        prior_strength=Decimal("20"),
        confidence_z=Decimal("0"),
        edge_threshold=Decimal("0"),
    )

    signals, _, _ = build_walk_forward_signals([*history, current], config=config)

    assert all(signal.candidate.market_id != current.market_id for signal in signals)


def test_economic_tape_keeps_one_sided_max_capacity() -> None:
    buy = _trade("buy-log", outcome="YES", price="0.65", minutes=-61, side="BUY")
    sell = _trade("sell-log", outcome="YES", price="0.65", minutes=-61, side="SELL")
    sell = replace(sell, tx_hash=buy.tx_hash, size=Decimal("120"))

    rows = _economic_trade_rows([buy, sell])

    assert len(rows) == 1
    assert rows[0].trade_id == "sell-log"
    assert rows[0].size == Decimal("120")
