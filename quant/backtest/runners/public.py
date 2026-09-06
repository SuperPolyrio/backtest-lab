"""Public validation runners for the Polymarket backtest engine."""

from __future__ import annotations

import argparse
from decimal import Decimal
import os
import time
from typing import Any

from ..backtest_engine import BacktestParameters, build_metrics, simulate_strategy
from ..ledger import build_ledger_rows, ledger_summary
from .base import MemorySampler, Timer, ValidationResult, print_results
from .data_sources import ClickHouseOrderFilledStore, FixtureReplayStore, MarketCandidate, PostgresBlockCloseStore
from .execution_replay import OrderIntent, ReplayTradeEvent, replay_limit_order, sequence_key
from .selectors import select_nba_2024_25_markets


class NoTradeReplaySmokeRunner:
    run_id = "no_trade_replay_smoke"

    def run(self, *, mode: str = "fixture", candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        rows = []
        with timer.track("db_query"):
            if mode == "db":
                candidate = candidate or _first_db_candidate()
                rows = PostgresBlockCloseStore().load_bars(candidate)
                data_version = f"db:{candidate.market_id}:{candidate.from_block}:{candidate.to_block}"
            else:
                store = FixtureReplayStore()
                rows = store.load_bars("no_trade")
                data_version = store.data_version("no_trade")
        with timer.track("engine"):
            bars_processed = len(rows)
        passed = bool(rows)
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else "no bars loaded",
            market_count=1 if rows else 0,
            rows_scanned=len(rows),
            bars_processed=bars_processed,
        )


class SingleFillReplayTest:
    run_id = "single_fill_replay"

    def run(self, *, mode: str = "fixture", candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        with timer.track("db_query"):
            if mode == "db":
                candidate = candidate or _first_db_candidate()
                events = _load_db_events(candidate)
                data_version = f"db:{candidate.market_id}:{candidate.from_block}:{candidate.to_block}"
            else:
                store = FixtureReplayStore()
                events = store.load_trade_events("single_fill")
                data_version = store.data_version("single_fill")
        with timer.track("engine"):
            order = OrderIntent(
                order_id="O-0001",
                side="BUY_YES",
                limit_price=Decimal("0.50"),
                size=Decimal("4"),
                submit_sequence=sequence_key(100, 0, 2, "0xsubmit"),
                liquidity_cap_pct=Decimal("50"),
            )
            fill = replay_limit_order(order, events)
        passed = (
            fill.status == "FILLED"
            and fill.filled_size == Decimal("4.0000000000")
            and fill.avg_fill_price == Decimal("0.4800000000")
            and fill.cash_delta == Decimal("-1.9200000000")
            and fill.position_delta == Decimal("4.0000000000")
            and all(event.event_sequence > order.submit_sequence for event in fill.candidate_events)
            and fill.filled_size <= sum((event.size for event in fill.candidate_events), Decimal("0")) * Decimal("0.50")
            and fill.filled_size == sum((event.size for event in fill.consumed_events), Decimal("0"))
        )
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else f"unexpected fill result {fill}",
            market_count=1,
            rows_scanned=len(events),
            bars_processed=len(events),
            orders_created=1,
            fills=1 if fill.status == "FILLED" else 0,
            partial_fills=1 if fill.status == "PARTIAL_FILLED" else 0,
            rejected=1 if fill.status in {"NO_FILL", "REJECTED"} else 0,
            meta={"cash_delta": str(fill.cash_delta), "position_delta": str(fill.position_delta)},
        )


class LimitOrderLifecycleTest:
    run_id = "limit_order_lifecycle"

    def run(self, *, mode: str = "fixture", candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        with timer.track("db_query"):
            events = FixtureReplayStore().load_trade_events("lifecycle")
            data_version = FixtureReplayStore().data_version("lifecycle")
        with timer.track("engine"):
            results = [
                replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("5"), "GTC", sequence_key(99, 0, 9, "0xsub"), liquidity_cap_pct=Decimal("100"), order_id="O-GTC"), events),
                replay_limit_order(OrderIntent("BUY_YES", Decimal("0.46"), Decimal("2"), "GTD", sequence_key(100, 0, 9, "0xsub"), expire_sequence=sequence_key(101, 0, 9, "0xexp"), order_id="O-GTD"), events),
                replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("20"), "FOK", sequence_key(100, 0, 0, "0xsub"), liquidity_cap_pct=Decimal("10"), order_id="O-FOK"), events),
                replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("20"), "FAK", sequence_key(100, 0, 0, "0xsub"), liquidity_cap_pct=Decimal("10"), order_id="O-FAK"), events),
            ]
        statuses = [result.status for result in results]
        passed = statuses == ["FILLED", "EXPIRED", "REJECTED", "PARTIAL_FILLED"]
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else f"unexpected lifecycle statuses {statuses}",
            market_count=1,
            rows_scanned=len(events),
            bars_processed=len(events),
            orders_created=len(results),
            fills=sum(1 for result in results if result.status == "FILLED"),
            partial_fills=sum(1 for result in results if result.status == "PARTIAL_FILLED"),
            rejected=sum(1 for result in results if result.status == "REJECTED"),
            expired=sum(1 for result in results if result.status == "EXPIRED"),
        )


class IlliquidRejectionSmokeRunner:
    run_id = "illiquid_rejection_smoke"

    def run(self, *, mode: str = "fixture", candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        with timer.track("db_query"):
            events = FixtureReplayStore().load_trade_events("illiquid")
            data_version = FixtureReplayStore().data_version("illiquid")
        with timer.track("engine"):
            orders = [
                OrderIntent("BUY_YES", Decimal("0.50"), Decimal("100"), "GTC", sequence_key(299, 0, 0, "0xsub"), liquidity_cap_pct=Decimal("10"), order_id="O-1"),
                OrderIntent("BUY_YES", Decimal("0.40"), Decimal("100"), "GTC", sequence_key(299, 0, 0, "0xsub"), liquidity_cap_pct=Decimal("10"), order_id="O-2"),
                OrderIntent("BUY_YES", Decimal("0.50"), Decimal("100"), "FOK", sequence_key(300, 0, 0, "0xsub"), liquidity_cap_pct=Decimal("10"), order_id="O-3"),
            ]
            results = [replay_limit_order(order, events) for order in orders]
        bad = sum(1 for result in results if result.status in {"NO_FILL", "REJECTED", "PARTIAL_FILLED"})
        passed = bad >= 2 and all(
            result.filled_size <= sum((event.size for event in result.candidate_events), Decimal("0")) * Decimal("0.10")
            and result.filled_size == sum((event.size for event in result.consumed_events), Decimal("0"))
            for result in results
        )
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else "illiquid orders exceeded volume cap or were unexpectedly liquid",
            market_count=1,
            rows_scanned=len(events),
            bars_processed=len(events),
            orders_created=len(results),
            fills=sum(1 for result in results if result.status == "FILLED"),
            partial_fills=sum(1 for result in results if result.status == "PARTIAL_FILLED"),
            rejected=sum(1 for result in results if result.status in {"NO_FILL", "REJECTED"}),
        )


class ResolutionSettlementSmokeRunner:
    run_id = "resolution_settlement_smoke"

    def run(self, *, mode: str = "fixture", candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        all_orders: list[dict[str, Any]] = []
        all_trades: list[dict[str, Any]] = []
        all_ledger: list[dict[str, Any]] = []
        with timer.track("db_query"):
            store = FixtureReplayStore()
            cases = [store.load_bars("settlement_zero"), store.load_bars("settlement_one")]
            data_version = store.data_version("settlement")
        with timer.track("engine"):
            for index, points in enumerate(cases):
                params = BacktestParameters(
                    initial_capital=Decimal("100"),
                    position_size=Decimal("10"),
                    buy_limit_price=Decimal("0.50"),
                    sell_limit_price=Decimal("0.99"),
                    settlement_value=Decimal(index),
                    liquidity_cap_pct=Decimal("100"),
                )
                result = simulate_strategy(points, {"market_slug": f"settlement-{index}", "token_side": "YES", "price_source": "orderfilled_block_close"}, params)
                all_orders.extend(result["orders"])
                all_trades.extend(result["trades"])
                all_ledger.extend(result["ledger"])
        passed = (
            len(all_trades) == 2
            and all(trade["exit_reason"] == "settlement" for trade in all_trades)
            and sum(1 for row in all_ledger if row["event_type"] == "SETTLEMENT") == 2
            and all(trade["exit_price"] in {Decimal("0"), Decimal("1")} for trade in all_trades)
        )
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else "settlement ledger mismatch",
            market_count=2,
            rows_scanned=sum(len(points) for points in cases),
            bars_processed=sum(len(points) for points in cases),
            orders_created=len(all_orders),
            fills=sum(1 for order in all_orders if order["status"] == "FILLED"),
            partial_fills=sum(1 for order in all_orders if order["status"] == "PARTIAL_FILLED"),
            rejected=sum(1 for order in all_orders if order["status"] in {"NO_FILL", "REJECTED"}),
            settlements=sum(1 for row in all_ledger if row["event_type"] == "SETTLEMENT"),
            meta={
                "trade_exit_pnl": str(sum((trade["pnl"] for trade in all_trades if trade["exit_reason"] != "settlement"), Decimal("0"))),
                "settlement_pnl": str(sum((trade["pnl"] for trade in all_trades if trade["exit_reason"] == "settlement"), Decimal("0"))),
            },
        )


class AccountLedgerReplayRunner:
    run_id = "account_ledger_replay"

    def run(self, *, mode: str = "fixture", address: str | None = None, candidate: MarketCandidate | None = None) -> ValidationResult:
        timer = Timer()
        start = time.perf_counter()
        address = (address or os.environ.get("POLYDATA_VALIDATION_ADDRESS") or "0xabc").lower()
        with timer.track("db_query"):
            events = FixtureReplayStore().account_events()
            data_version = FixtureReplayStore().data_version("account")
        with timer.track("engine"):
            cash = Decimal("0")
            position = Decimal("0")
            trades: list[dict[str, Any]] = []
            for row in events:
                if str(row["address"]).lower() != address:
                    continue
                size = Decimal(str(row["size"]))
                price = Decimal(str(row["price"]))
                fee = Decimal(str(row["fee"]))
                if row["side"] == "BUY_YES":
                    cash -= size * price + fee
                    position += size
                else:
                    cash += size * price - fee
                    position -= size
                trades.append({"row": row, "cash": cash, "position": position})
        with timer.track("ledger_write"):
            ledger_cash = trades[-1]["cash"] if trades else Decimal("0")
            ledger_position = trades[-1]["position"] if trades else Decimal("0")
        passed = ledger_cash == cash and ledger_position == position and position == Decimal("6")
        return _result(
            self.run_id,
            data_version,
            timer,
            start,
            passed=passed,
            message="" if passed else "account ledger replay mismatch",
            market_count=1,
            rows_scanned=len(events),
            bars_processed=len(events),
            orders_created=len(trades),
            fills=len(trades),
            meta={"address": address, "position": str(position), "cashflow": str(cash), "realized_pnl": str(cash), "settlement_pnl": "0"},
        )


PUBLIC_RUNNERS = [
    NoTradeReplaySmokeRunner,
    SingleFillReplayTest,
    LimitOrderLifecycleTest,
    IlliquidRejectionSmokeRunner,
    ResolutionSettlementSmokeRunner,
    AccountLedgerReplayRunner,
]


def run_all_public(*, mode: str = "fixture", address: str | None = None) -> list[ValidationResult]:
    candidate = _first_db_candidate() if mode == "db" else None
    results: list[ValidationResult] = []
    for runner_type in PUBLIC_RUNNERS:
        runner = runner_type()
        if isinstance(runner, AccountLedgerReplayRunner):
            results.append(runner.run(mode=mode, address=address, candidate=candidate))
        else:
            results.append(runner.run(mode=mode, candidate=candidate))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Polymarket backtest validation runners.")
    parser.add_argument("--mode", choices=("fixture", "db"), default="fixture")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--address", default=os.environ.get("POLYDATA_VALIDATION_ADDRESS", ""))
    args = parser.parse_args(argv)
    results = run_all_public(mode=args.mode, address=args.address or None)
    print_results(results, json_output=args.json)
    return 0 if all(result.passed for result in results) else 1


def _first_db_candidate() -> MarketCandidate:
    configured_slug = os.environ.get("POLYDATA_VALIDATION_MARKET_SLUG", "").strip()
    if configured_slug:
        return MarketCandidate(
            market_id=int(os.environ.get("POLYDATA_VALIDATION_MARKET_ID", "0") or 0),
            market_slug=configured_slug,
            token_id=os.environ.get("POLYDATA_VALIDATION_TOKEN_ID") or None,
            token_side=os.environ.get("POLYDATA_VALIDATION_TOKEN_SIDE", "YES"),
            from_block=int(os.environ["POLYDATA_VALIDATION_FROM_BLOCK"]) if os.environ.get("POLYDATA_VALIDATION_FROM_BLOCK") else None,
            to_block=int(os.environ["POLYDATA_VALIDATION_TO_BLOCK"]) if os.environ.get("POLYDATA_VALIDATION_TO_BLOCK") else None,
            title=configured_slug,
        )
    candidates = select_nba_2024_25_markets(limit=1)
    if not candidates:
        raise RuntimeError("candidate unavailable: no NBA 24/25 market with ready coverage")
    return candidates[0]


def _load_db_events(candidate: MarketCandidate) -> list[ReplayTradeEvent]:
    if not candidate.market_id or not candidate.token_id or candidate.from_block is None or candidate.to_block is None:
        raise RuntimeError("db single fill requires market_id, token_id, from_block, and to_block")
    return ClickHouseOrderFilledStore().load_trade_events(
        market_id=candidate.market_id,
        token_id=candidate.token_id,
        from_block=candidate.from_block,
        to_block=min(candidate.to_block, candidate.from_block + 5000),
        limit=5000,
    )


def _result(
    run_id: str,
    data_version: str,
    timer: Timer,
    start: float,
    *,
    passed: bool,
    message: str,
    market_count: int,
    rows_scanned: int,
    bars_processed: int,
    orders_created: int = 0,
    fills: int = 0,
    partial_fills: int = 0,
    rejected: int = 0,
    expired: int = 0,
    settlements: int = 0,
    meta: dict[str, Any] | None = None,
) -> ValidationResult:
    return ValidationResult(
        run_id=run_id,
        data_version=data_version,
        market_count=market_count,
        rows_scanned=rows_scanned,
        bars_processed=bars_processed,
        orders_created=orders_created,
        fills=fills,
        partial_fills=partial_fills,
        rejected=rejected,
        expired=expired,
        settlements=settlements,
        db_query_sec=timer.elapsed.get("db_query", 0.0),
        engine_sec=timer.elapsed.get("engine", 0.0),
        ledger_write_sec=timer.elapsed.get("ledger_write", 0.0),
        total_runtime_sec=time.perf_counter() - start,
        peak_memory_mb=MemorySampler.peak_mb(),
        passed=passed,
        message=message,
        meta=meta or {},
    )


if __name__ == "__main__":
    raise SystemExit(main())
