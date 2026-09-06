"""Backtest framework adapters for quant price rows.

The production tables use one result shape regardless of the execution engine.
This module keeps framework-specific imports and bar conversion out of the
database runner so queued jobs can switch engines per run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Callable

from .execution import snapshot_from_any
from .l2_orderfilled_execution import combine_orderfilled_l2_execution, l2_config_from_params, simulate_l2_depth_execution
from . import orderfilled_execution


SUPPORTED_BACKTEST_ENGINES = {"builtin", "backtrader", "nautilus_trader"}
ORDERFILLED_CROSS_MODES = {"ORDERFILLED_CROSS", "ORDERFILLED_LIMIT_REPLAY", "LIMIT_REPLAY"}
ORDERFILLED_LOB_MODES = {"ORDERFILLED_LOB", "ORDERFILLED_DEPTH", "FILL_LOB", "ORDERFILLED_LOB_CALIBRATED"}


@dataclass
class AdapterPosition:
    trade_index: int
    entry_index: int
    entry_x: int
    entry_price: Decimal
    size: Decimal
    requested_notional: Decimal
    filled_notional: Decimal
    fill_pct: Decimal
    fill_status: str = "FILLED"
    book_snapshot_id: int | None = None
    snapshot_version: str | None = None
    staleness_seconds: Decimal | None = None
    staleness_blocks: int | None = None
    avg_fill_price: Decimal | None = None
    fill_probability: Decimal = Decimal("0")
    block_volume: Decimal = Decimal("0")
    trade_count: int = 0
    available_notional: Decimal = Decimal("0")
    entry_fee_cost: Decimal = Decimal("0")
    entry_rebate: Decimal = Decimal("0")
    entry_slippage_cost: Decimal = Decimal("0")


def normalize_backtest_engine(value: Any) -> str:
    text = str(value or "builtin").strip().lower().replace("-", "_")
    aliases = {
        "": "builtin",
        "internal": "builtin",
        "fixed_threshold": "builtin",
        "fixed_threshold_v1": "builtin",
        "native": "builtin",
        "bt": "backtrader",
        "back_trader": "backtrader",
        "mementum_backtrader": "backtrader",
        "nautilus": "nautilus_trader",
        "nautilus_trader": "nautilus_trader",
        "nautech_nautilus_trader": "nautilus_trader",
    }
    engine = aliases.get(text, text)
    if engine not in SUPPORTED_BACKTEST_ENGINES:
        raise ValueError(f"unsupported backtest_engine: {value!r}")
    return engine


def run_framework_backtest(
    engine: str,
    points: list[Any],
    run: dict[str, Any],
    params: Any,
    *,
    builtin_simulator: Callable[[list[Any], dict[str, Any], Any], dict[str, Any]],
    metrics_builder: Callable[[list[dict[str, Any]], list[dict[str, Any]], list[Any], Any], list[dict[str, Any]]],
) -> dict[str, Any]:
    """Execute a normalized quant backtest with the requested framework."""

    requested_engine = normalize_backtest_engine(engine)
    actual_engine = requested_engine
    execution_mode = str(getattr(params, "execution_price_mode", "") or "").strip().upper().replace("-", "_")
    if requested_engine != "builtin" and execution_mode in ORDERFILLED_CROSS_MODES:
        actual_engine = "builtin"
        result = builtin_simulator(points, run, params)
        result.setdefault("events", []).append({
            "event_type": "engine_routed_to_builtin",
            "x_axis": "block_number" if run.get("price_source") == "orderfilled_block_close" else "timestamp",
            "x_value": int(getattr(points[0], "x_value", 0)) if points else 0,
            "trade_id": None,
            "price": Decimal("0"),
            "message": f"{requested_engine} adapter does not implement passive limit replay; used builtin limit replay engine",
            "meta": {"requested_engine": requested_engine, "actual_engine": actual_engine, "execution_price_mode": execution_mode},
        })
        result["engine_routed_to_builtin"] = True
    elif requested_engine == "builtin":
        result = builtin_simulator(points, run, params)
    elif requested_engine == "backtrader":
        result = _run_backtrader(points, run, params, metrics_builder)
    elif requested_engine == "nautilus_trader":
        result = _run_nautilus_trader(points, run, params, metrics_builder)
    else:  # pragma: no cover - guarded by normalize_backtest_engine
        raise ValueError(f"unsupported backtest_engine: {requested_engine!r}")
    result["requested_backtest_engine"] = requested_engine
    result["actual_backtest_engine"] = actual_engine
    result.setdefault("engine_routed_to_builtin", False)
    _annotate_result(result, requested_engine, actual_engine)
    return result


def _run_backtrader(
    points: list[Any],
    run: dict[str, Any],
    params: Any,
    metrics_builder: Callable[[list[dict[str, Any]], list[dict[str, Any]], list[Any], Any], list[dict[str, Any]]],
) -> dict[str, Any]:
    bt = _import_external_package("backtrader", env_var="POLYDATA_BACKTRADER_PATH", repo_name="backtrader")
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover - depends on deployment env
        raise RuntimeError("backtrader engine requires pandas to build the in-memory bar feed") from exc

    frame = pd.DataFrame(
        {
            "open": [float(point.price) for point in points],
            "high": [float(point.price) for point in points],
            "low": [float(point.price) for point in points],
            "close": [float(point.price) for point in points],
            "volume": [float(point.volume) for point in points],
        },
        index=pd.date_range("2000-01-01", periods=len(points), freq="min", tz="UTC"),
    )
    data = bt.feeds.PandasData(dataname=frame)
    x_values = [int(point.x_value) for point in points]
    x_axis = _x_axis(run)
    quant_params = params

    class QuantThresholdStrategy(bt.Strategy):  # type: ignore[misc]
        params = (
            ("x_values", x_values),
            ("x_axis", x_axis),
            ("run_row", run),
            ("quant_params", quant_params),
            ("price_points", points),
            ("metrics_builder", metrics_builder),
        )

        def __init__(self) -> None:
            self.open_position: AdapterPosition | None = None
            self.trades: list[dict[str, Any]] = []
            self.events: list[dict[str, Any]] = []
            self.equity_rows: list[dict[str, Any]] = []
            self.realized_equity = self.p.quant_params.initial_capital
            self.peak_equity = self.p.quant_params.initial_capital

        def next(self) -> None:
            index = len(self.data) - 1
            x_value = int(self.p.x_values[index])
            point = self.p.price_points[index]
            price = Decimal(str(self.data.close[0]))
            if self.open_position is None and price >= self.p.quant_params.entry_threshold:
                fill = _fill_decision(self.p.quant_params, point, self.p.run_row, "BUY_YES")
                if fill["size"] <= 0:
                    self._record_equity(index, x_value, price)
                    return
                self.open_position = AdapterPosition(
                    trade_index=len(self.trades) + 1,
                    entry_index=index,
                    entry_x=x_value,
                    entry_price=fill.get("entry_price") or _execution_price(price, self.p.quant_params, "entry"),
                    size=fill["size"],
                    requested_notional=fill["requested_notional"],
                    filled_notional=fill["filled_notional"],
                    fill_pct=fill["fill_pct"],
                    fill_status=fill.get("fill_status", "FILLED"),
                    book_snapshot_id=fill.get("book_snapshot_id"),
                    snapshot_version=fill.get("snapshot_version"),
                    staleness_seconds=fill.get("staleness_seconds"),
                    staleness_blocks=fill.get("staleness_blocks"),
                    avg_fill_price=fill.get("avg_fill_price"),
                    fill_probability=fill.get("fill_probability", Decimal("0")),
                    block_volume=fill.get("block_volume", Decimal(str(getattr(point, "volume", "0") or "0"))),
                    trade_count=int(fill.get("trade_count", getattr(point, "trade_count", 0)) or 0),
                    available_notional=fill.get("available_notional", Decimal("0")),
                    entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
                )
                self.events.append(_event("open", self.p.x_axis, x_value, f"T-{self.open_position.trade_index:04d}", price, "entry threshold reached"))
            elif self.open_position is not None:
                exit_reason = _exit_reason(price, self.open_position.entry_price, index - self.open_position.entry_index, self.p.quant_params)
                if exit_reason:
                    exit_fill = _fill_decision(self.p.quant_params, point, self.p.run_row, "SELL_YES", target_size=self.open_position.size)
                    if exit_fill["size"] <= 0:
                        self.events.append(_event("exit_rejected", self.p.x_axis, x_value, f"T-{self.open_position.trade_index:04d}", price, exit_reason))
                        self._record_equity(index, x_value, price)
                        return
                    trade = _close_trade(self.p.run_row, self.p.x_axis, self.open_position, x_value, price, index, exit_reason, self.p.quant_params, exit_fill=exit_fill)
                    self.trades.append(trade)
                    self.realized_equity += trade["pnl"]
                    self.events.append(_event("close", self.p.x_axis, x_value, trade["trade_id"], price, exit_reason))
                    remaining_size = (self.open_position.size - exit_fill["size"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                    self.open_position.size = remaining_size
                    if remaining_size <= 0:
                        self.open_position = None
                    else:
                        self.open_position.trade_index = len(self.trades) + 1
            self._record_equity(index, x_value, price)

        def stop(self) -> None:
            if self.open_position is not None:
                index = len(self.p.price_points) - 1
                point = self.p.price_points[index]
                exit_fill = _fill_decision(self.p.quant_params, point, self.p.run_row, "SELL_YES", target_size=self.open_position.size)
                if exit_fill["size"] > 0:
                    trade = _close_trade(self.p.run_row, self.p.x_axis, self.open_position, int(point.x_value), point.price, index, "end_of_data", self.p.quant_params, exit_fill=exit_fill)
                    self.trades.append(trade)
                    self.realized_equity += trade["pnl"]
                    self.events.append(_event("close", self.p.x_axis, int(point.x_value), trade["trade_id"], point.price, "end_of_data"))
                else:
                    self.events.append(_event("force_close_rejected", self.p.x_axis, int(point.x_value), f"T-{self.open_position.trade_index:04d}", point.price, "end_of_data"))
                self.open_position = None
            self.result = {
                "trades": self.trades,
                "equity": self.equity_rows,
                "metrics": self.p.metrics_builder(self.trades, self.equity_rows, self.p.price_points, self.p.quant_params),
                "events": self.events,
            }

        def _record_equity(self, index: int, x_value: int, price: Decimal) -> None:
            mark_equity = self.realized_equity
            if self.open_position is not None:
                mark_equity += (_execution_price(price, self.p.quant_params, "exit") - self.open_position.entry_price) * self.open_position.size
            self.peak_equity = max(self.peak_equity, mark_equity)
            drawdown = mark_equity - self.peak_equity
            self.equity_rows.append(
                {
                    "point_index": index + 1,
                    "x_axis": self.p.x_axis,
                    "x_value": x_value,
                    "equity": mark_equity,
                    "drawdown": drawdown,
                    "drawdown_pct": _pct(drawdown, self.peak_equity),
                    "cumulative_return": _pct(mark_equity - self.p.quant_params.initial_capital, self.p.quant_params.initial_capital),
                }
            )

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(data)
    cerebro.addstrategy(QuantThresholdStrategy)
    strategies = cerebro.run()
    if not strategies or strategies[0] is None:
        raise RuntimeError("backtrader did not return a completed strategy")
    return strategies[0].result


def _run_nautilus_trader(
    points: list[Any],
    run: dict[str, Any],
    params: Any,
    metrics_builder: Callable[[list[dict[str, Any]], list[dict[str, Any]], list[Any], Any], list[dict[str, Any]]],
) -> dict[str, Any]:
    if sys.version_info < (3, 12):
        return _run_nautilus_trader_subprocess(points, run, params)

    _import_external_package("nautilus_trader", env_var="POLYDATA_NAUTILUS_TRADER_PATH", repo_name="nautilus_trader")
    try:
        from nautilus_trader.backtest.engine import BacktestEngine
        from nautilus_trader.config import BacktestEngineConfig, LoggingConfig, StrategyConfig
        from nautilus_trader.core.datetime import dt_to_unix_nanos
        from nautilus_trader.model.currencies import USD
        from nautilus_trader.model.data import Bar, BarSpecification, BarType
        from nautilus_trader.model.enums import AccountType, BarAggregation, OmsType, PriceType
        from nautilus_trader.model.identifiers import InstrumentId, Venue
        from nautilus_trader.model.objects import Money, Quantity
        from nautilus_trader.test_kit.providers import TestInstrumentProvider
        from nautilus_trader.trading.strategy import Strategy
    except Exception as exc:  # pragma: no cover - depends on installed wheel/Python version
        raise RuntimeError(
            "nautilus_trader engine requires a working nautilus_trader install "
            "(Python 3.12+ wheels or a built source checkout)"
        ) from exc

    x_values = [int(point.x_value) for point in points]
    x_axis = _x_axis(run)
    venue = Venue("SIM")
    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD", venue)
    bar_type = BarType(
        instrument_id=instrument.id,
        bar_spec=BarSpecification(step=1, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST),
    )
    base_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
    bars = []
    for index, point in enumerate(points):
        timestamp_ns = dt_to_unix_nanos(base_dt + timedelta(minutes=index))
        price = instrument.make_price(point.price)
        bars.append(
            Bar(
                bar_type=bar_type,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Quantity.from_str(str(max(Decimal("0"), point.volume))),
                ts_event=timestamp_ns,
                ts_init=timestamp_ns,
            )
        )

    class QuantThresholdConfig(StrategyConfig, frozen=True):
        instrument_id: InstrumentId
        bar_type: BarType
        x_values: list[int]
        x_axis: str
        run_row: dict[str, Any]
        quant_params: Any
        price_points: list[Any]
        metrics_builder: Any

    class QuantThresholdStrategy(Strategy):  # type: ignore[misc]
        def __init__(self, config: QuantThresholdConfig):
            super().__init__(config)
            self.index = -1
            self.open_position: AdapterPosition | None = None
            self.trades: list[dict[str, Any]] = []
            self.events: list[dict[str, Any]] = []
            self.equity_rows: list[dict[str, Any]] = []
            self.realized_equity = config.quant_params.initial_capital
            self.peak_equity = config.quant_params.initial_capital

        def on_start(self) -> None:
            self.subscribe_bars(self.config.bar_type)

        def on_bar(self, bar: Bar) -> None:
            self.index += 1
            x_value = int(self.config.x_values[self.index])
            point = self.config.price_points[self.index]
            price = Decimal(str(bar.close))
            if self.open_position is None and price >= self.config.quant_params.entry_threshold:
                fill = _fill_decision(self.config.quant_params, point, self.config.run_row, "BUY_YES")
                if fill["size"] <= 0:
                    self._record_equity(x_value, price)
                    return
                self.open_position = AdapterPosition(
                    trade_index=len(self.trades) + 1,
                    entry_index=self.index,
                    entry_x=x_value,
                    entry_price=fill.get("entry_price") or _execution_price(price, self.config.quant_params, "entry"),
                    size=fill["size"],
                    requested_notional=fill["requested_notional"],
                    filled_notional=fill["filled_notional"],
                    fill_pct=fill["fill_pct"],
                    fill_status=fill.get("fill_status", "FILLED"),
                    book_snapshot_id=fill.get("book_snapshot_id"),
                    snapshot_version=fill.get("snapshot_version"),
                    staleness_seconds=fill.get("staleness_seconds"),
                    staleness_blocks=fill.get("staleness_blocks"),
                    avg_fill_price=fill.get("avg_fill_price"),
                    fill_probability=fill.get("fill_probability", Decimal("0")),
                    block_volume=fill.get("block_volume", Decimal(str(getattr(point, "volume", "0") or "0"))),
                    trade_count=int(fill.get("trade_count", getattr(point, "trade_count", 0)) or 0),
                    available_notional=fill.get("available_notional", Decimal("0")),
                    entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
                )
                self.events.append(_event("open", self.config.x_axis, x_value, f"T-{self.open_position.trade_index:04d}", price, "entry threshold reached"))
            elif self.open_position is not None:
                exit_reason = _exit_reason(price, self.open_position.entry_price, self.index - self.open_position.entry_index, self.config.quant_params)
                if exit_reason:
                    exit_fill = _fill_decision(self.config.quant_params, point, self.config.run_row, "SELL_YES", target_size=self.open_position.size)
                    if exit_fill["size"] <= 0:
                        self.events.append(_event("exit_rejected", self.config.x_axis, x_value, f"T-{self.open_position.trade_index:04d}", price, exit_reason))
                        self._record_equity(x_value, price)
                        return
                    trade = _close_trade(self.config.run_row, self.config.x_axis, self.open_position, x_value, price, self.index, exit_reason, self.config.quant_params, exit_fill=exit_fill)
                    self.trades.append(trade)
                    self.realized_equity += trade["pnl"]
                    self.events.append(_event("close", self.config.x_axis, x_value, trade["trade_id"], price, exit_reason))
                    remaining_size = (self.open_position.size - exit_fill["size"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                    self.open_position.size = remaining_size
                    if remaining_size <= 0:
                        self.open_position = None
                    else:
                        self.open_position.trade_index = len(self.trades) + 1
            self._record_equity(x_value, price)

        def on_stop(self) -> None:
            if self.open_position is not None:
                point = self.config.price_points[-1]
                exit_fill = _fill_decision(self.config.quant_params, point, self.config.run_row, "SELL_YES", target_size=self.open_position.size)
                if exit_fill["size"] > 0:
                    trade = _close_trade(self.config.run_row, self.config.x_axis, self.open_position, int(point.x_value), point.price, len(self.config.price_points) - 1, "end_of_data", self.config.quant_params, exit_fill=exit_fill)
                    self.trades.append(trade)
                    self.realized_equity += trade["pnl"]
                    self.events.append(_event("close", self.config.x_axis, int(point.x_value), trade["trade_id"], point.price, "end_of_data"))
                else:
                    self.events.append(_event("force_close_rejected", self.config.x_axis, int(point.x_value), f"T-{self.open_position.trade_index:04d}", point.price, "end_of_data"))
                self.open_position = None
            self.result = {
                "trades": self.trades,
                "equity": self.equity_rows,
                "metrics": self.config.metrics_builder(self.trades, self.equity_rows, self.config.price_points, self.config.quant_params),
                "events": self.events,
            }

        def _record_equity(self, x_value: int, price: Decimal) -> None:
            mark_equity = self.realized_equity
            if self.open_position is not None:
                mark_equity += (_execution_price(price, self.config.quant_params, "exit") - self.open_position.entry_price) * self.open_position.size
            self.peak_equity = max(self.peak_equity, mark_equity)
            drawdown = mark_equity - self.peak_equity
            self.equity_rows.append(
                {
                    "point_index": self.index + 1,
                    "x_axis": self.config.x_axis,
                    "x_value": x_value,
                    "equity": mark_equity,
                    "drawdown": drawdown,
                    "drawdown_pct": _pct(drawdown, self.peak_equity),
                    "cumulative_return": _pct(mark_equity - self.config.quant_params.initial_capital, self.config.quant_params.initial_capital),
                }
            )

    engine = BacktestEngine(config=BacktestEngineConfig(logging=LoggingConfig(log_level="ERROR")))
    try:
        engine.add_venue(
            venue=venue,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money(params.initial_capital, USD)],
            base_currency=USD,
        )
        engine.add_instrument(instrument)
        engine.add_data(bars)
        strategy = QuantThresholdStrategy(
            QuantThresholdConfig(
                instrument_id=instrument.id,
                bar_type=bar_type,
                x_values=x_values,
                x_axis=x_axis,
                run_row=run,
                quant_params=params,
                price_points=points,
                metrics_builder=metrics_builder,
            )
        )
        engine.add_strategy(strategy)
        engine.run()
        return strategy.result
    finally:
        engine.dispose()


def _run_nautilus_trader_subprocess(points: list[Any], run: dict[str, Any], params: Any) -> dict[str, Any]:
    python_bin = _nautilus_python_bin()
    payload = {
        "points": [
            {
                "x_value": int(point.x_value),
                "price": str(point.price),
                "volume": str(point.volume),
                "trade_count": int(getattr(point, "trade_count", 0) or 0),
                "timestamp": point.timestamp.isoformat() if getattr(point, "timestamp", None) else None,
            }
            for point in points
        ],
        "run": run,
        "params": {
            "entry_threshold": str(params.entry_threshold),
            "exit_threshold": str(params.exit_threshold),
            "stop_loss": str(params.stop_loss),
            "take_profit": str(params.take_profit),
            "max_holding_bars": int(params.max_holding_bars),
            "initial_capital": str(params.initial_capital),
            "position_size": str(params.position_size),
            "fee_bps": str(getattr(params, "fee_bps", "0")),
            "slippage_bps": str(getattr(params, "slippage_bps", "0")),
            "liquidity_cap_pct": str(getattr(params, "liquidity_cap_pct", "100")),
            "max_position_notional": str(getattr(params, "max_position_notional", "0")),
            "min_fill_pct": str(getattr(params, "min_fill_pct", "0")),
            "execution_price_mode": str(getattr(params, "execution_price_mode", "ORDERFILLED_CROSS")),
            "latency_seconds": str(getattr(params, "latency_seconds", "0")),
            "max_book_staleness_seconds": str(getattr(params, "max_book_staleness_seconds", "900")),
            "allow_partial_fill": bool(getattr(params, "allow_partial_fill", True)),
            "min_fill_size": str(getattr(params, "min_fill_size", "0")),
            "reject_on_stale_book": bool(getattr(params, "reject_on_stale_book", True)),
            "final_valuation_mode": str(getattr(params, "final_valuation_mode", "SETTLEMENT")),
            "buy_limit_price": "" if getattr(params, "buy_limit_price", None) is None else str(getattr(params, "buy_limit_price")),
            "sell_limit_price": "" if getattr(params, "sell_limit_price", None) is None else str(getattr(params, "sell_limit_price")),
            "settlement_value": "" if getattr(params, "settlement_value", None) is None else str(getattr(params, "settlement_value")),
            "max_entry_price": str(getattr(params, "max_entry_price", "1")),
            "min_exit_price": str(getattr(params, "min_exit_price", "0")),
        },
    }
    project_root = Path(__file__).resolve().parents[2]
    worker = Path(__file__).resolve().parent / "nautilus_worker.py"
    with tempfile.TemporaryDirectory(prefix="quant-nautilus-") as tmpdir:
        input_path = Path(tmpdir) / "input.json"
        output_path = Path(tmpdir) / "output.json"
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        completed = subprocess.run(
            [str(python_bin), str(worker), str(input_path), str(output_path)],
            cwd=str(project_root),
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            error = (completed.stderr or completed.stdout or "nautilus worker failed").strip()
            raise RuntimeError(error[:4000])
        if not output_path.exists():
            raise RuntimeError("nautilus worker did not write an output file")
        return _decode_result(json.loads(output_path.read_text(encoding="utf-8")))


def _nautilus_python_bin() -> Path:
    configured = os.environ.get("POLYDATA_NAUTILUS_PYTHON")
    candidates = [
        Path(configured).expanduser() if configured else None,
        Path.home() / ".conda/envs/polymonitor-nautilus312/bin/python",
        Path("/opt/anaconda3/envs/polymonitor-nautilus312/bin/python"),
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate.resolve()
    raise RuntimeError(
        "nautilus_trader requires a Python 3.12 runtime. Set POLYDATA_NAUTILUS_PYTHON "
        "to the conda env python path."
    )


_DECIMAL_RESULT_KEYS = {
    "price",
    "equity",
    "drawdown",
    "drawdown_pct",
    "cumulative_return",
    "value",
    "entry_price",
    "exit_price",
    "size",
    "notional",
    "requested_notional",
    "filled_notional",
    "requested_size",
    "filled_size",
    "unfilled_size",
    "fill_pct",
    "avg_fill_price",
    "staleness_seconds",
    "fee_cost",
    "slippage_cost",
    "execution_cost",
    "pnl",
    "pnl_pct",
}


def _decode_result(value: Any, key: str | None = None) -> Any:
    if isinstance(value, list):
        return [_decode_result(item, key) for item in value]
    if isinstance(value, dict):
        return {item_key: _decode_result(item_value, item_key) for item_key, item_value in value.items()}
    if key in _DECIMAL_RESULT_KEYS and value is not None:
        return Decimal(str(value))
    return value


def _import_external_package(package_name: str, *, env_var: str, repo_name: str) -> Any:
    try:
        return importlib.import_module(package_name)
    except Exception as first_exc:
        repo_path = _external_repo_path(env_var, repo_name)
        if repo_path and str(repo_path) not in sys.path:
            sys.path.insert(0, str(repo_path))
        try:
            return importlib.import_module(package_name)
        except Exception as second_exc:
            raise RuntimeError(
                f"{package_name} is not importable. Install it in this Python environment "
                f"or set {env_var} to the cloned repository path."
            ) from second_exc if repo_path else first_exc


def _external_repo_path(env_var: str, repo_name: str) -> Path | None:
    configured = os.environ.get(env_var)
    if configured:
        path = Path(configured).expanduser().resolve()
        return path if path.exists() else None
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / repo_name
        if candidate.exists():
            return candidate
        sibling = parent.parent / repo_name
        if sibling.exists():
            return sibling
    return None


def _annotate_result(result: dict[str, Any], requested_engine: str, actual_engine: str) -> None:
    events = result.setdefault("events", [])
    if requested_engine == actual_engine:
        message = f"backtest engine: {actual_engine}"
    else:
        message = f"requested engine: {requested_engine}; actual engine: {actual_engine}"
    events.insert(0, _event("framework", "engine", 0, None, Decimal("0"), message))
    metrics = result.setdefault("metrics", [])
    metrics.append(
        {
            "metric_key": "backtest_engine",
            "metric_name": "Backtest Engine",
            "metric_group": "system",
            "value": Decimal("0"),
            "formatted_value": requested_engine,
            "delta": "requested",
            "status": "neutral",
            "tooltip": "Execution framework requested for this run",
            "sort_order": 10_000,
        }
    )
    metrics.append(
        {
            "metric_key": "actual_backtest_engine",
            "metric_name": "Actual Backtest Engine",
            "metric_group": "system",
            "value": Decimal("0"),
            "formatted_value": actual_engine,
            "delta": "execution",
            "status": "neutral",
            "tooltip": "Execution engine that actually ran after adapter routing",
            "sort_order": 10_001,
        }
    )


def _x_axis(run: dict[str, Any]) -> str:
    return "timestamp" if run["price_source"] == "frontend" else "block_number"


def _exit_reason(price: Decimal, entry_price: Decimal, holding_bars: int, params: Any) -> str | None:
    if price <= params.exit_threshold:
        return "exit_threshold"
    if price <= entry_price * (Decimal("1") - params.stop_loss):
        return "stop_loss"
    if price >= entry_price * (Decimal("1") + params.take_profit):
        return "take_profit"
    if holding_bars >= params.max_holding_bars:
        return "max_holding_bars"
    return None


def _close_trade(
    run: dict[str, Any],
    x_axis: str,
    position: AdapterPosition,
    exit_x: int,
    exit_price: Decimal,
    point_index: int,
    exit_reason: str,
    params: Any,
    *,
    exit_fill: dict[str, Any] | None = None,
) -> dict[str, Any]:
    close_size = Decimal(str(exit_fill.get("size"))) if exit_fill else position.size
    fill_exit_price = Decimal(str(exit_fill.get("exit_price") or exit_fill.get("avg_fill_price"))) if exit_fill and (exit_fill.get("exit_price") or exit_fill.get("avg_fill_price")) else _execution_price(exit_price, params, "exit")
    notional = position.entry_price * close_size
    entry_ratio = (close_size / position.size) if position.size > 0 else Decimal("1")
    entry_fee_cost = (position.entry_fee_cost * entry_ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    entry_rebate = (position.entry_rebate * entry_ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    entry_slippage = (position.entry_slippage_cost * entry_ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    exit_fee_cost = Decimal(str(exit_fill.get("fee_cost") or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if exit_fill else Decimal("0")
    exit_rebate = Decimal(str(exit_fill.get("rebate") or exit_fill.get("rebate_cost") or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if exit_fill else Decimal("0")
    exit_slippage = Decimal(str(exit_fill.get("slippage_cost") or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if exit_fill else ((exit_price - fill_exit_price) * close_size).copy_abs().quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fee_cost = (entry_fee_cost + exit_fee_cost).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    rebate = (entry_rebate + exit_rebate).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    slippage_cost = (entry_slippage + exit_slippage).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    execution_cost = (fee_cost + slippage_cost - rebate).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    pnl = (fill_exit_price - position.entry_price) * close_size - fee_cost + rebate
    return {
        "trade_id": f"T-{position.trade_index:04d}",
        "market_slug": run["market_slug"],
        "token_side": run["token_side"],
        "side": "LONG",
        "x_axis": x_axis,
        "entry_x": position.entry_x,
        "exit_x": exit_x,
        "entry_price": position.entry_price,
        "exit_price": fill_exit_price,
        "size": close_size,
        "notional": notional,
        "requested_notional": getattr(position, "requested_notional", notional),
        "filled_notional": getattr(position, "filled_notional", notional),
        "requested_size": position.size,
        "filled_size": close_size,
        "unfilled_size": max(Decimal("0"), position.size - close_size),
        "fill_pct": getattr(position, "fill_pct", Decimal("100")),
        "fill_status": exit_fill.get("fill_status") if exit_fill else getattr(position, "fill_status", "FILLED"),
        "book_snapshot_id": (exit_fill.get("book_snapshot_id") or getattr(position, "book_snapshot_id", None)) if exit_fill else getattr(position, "book_snapshot_id", None),
        "snapshot_version": (exit_fill.get("snapshot_version") or getattr(position, "snapshot_version", None)) if exit_fill else getattr(position, "snapshot_version", None),
        "staleness_seconds": exit_fill.get("staleness_seconds") if exit_fill and exit_fill.get("staleness_seconds") is not None else getattr(position, "staleness_seconds", None),
        "staleness_blocks": exit_fill.get("staleness_blocks") if exit_fill and exit_fill.get("staleness_blocks") is not None else getattr(position, "staleness_blocks", None),
        "fill_probability": exit_fill.get("fill_probability") if exit_fill else getattr(position, "fill_probability", Decimal("0")),
        "block_volume": exit_fill.get("block_volume") if exit_fill else getattr(position, "block_volume", Decimal("0")),
        "trade_count": exit_fill.get("trade_count") if exit_fill else getattr(position, "trade_count", 0),
        "available_notional": exit_fill.get("available_notional") if exit_fill else getattr(position, "available_notional", Decimal("0")),
        "execution_source": exit_fill.get("execution_source") if exit_fill else "unknown",
        "avg_fill_price": fill_exit_price,
        "pnl": pnl.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "pnl_pct": _pct(pnl, notional),
        "holding_bars": max(1, point_index - position.entry_index),
        "exit_reason": exit_reason,
        "fee_cost": fee_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "rebate": rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "slippage_cost": slippage_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "execution_cost": execution_cost,
    }


def _event(event_type: str, x_axis: str, x_value: int, trade_id: str | None, price: Decimal, message: str) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "x_axis": x_axis,
        "x_value": x_value,
        "trade_id": trade_id,
        "price": price,
        "message": message,
        "meta": {},
    }


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if not denominator:
        return Decimal("0")
    return (numerator / denominator * Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _bps_fraction(value: Any) -> Decimal:
    return orderfilled_execution.bps_fraction(value)


def _role_fee_bps(params: Any, role: str) -> Decimal:
    return orderfilled_execution.role_fee_bps(params, role)


def _role_rebate_bps(params: Any, role: str) -> Decimal:
    return orderfilled_execution.role_rebate_bps(params, role)


def _fee_rebate_for_notional(params: Any, notional: Decimal, role: str) -> tuple[Decimal, Decimal]:
    return orderfilled_execution.fee_rebate_for_notional(params, notional, role)


def _execution_price(price: Decimal, params: Any, side: str) -> Decimal:
    return orderfilled_execution.execution_price(price, params, side)


def _target_notional(params: Any) -> Decimal:
    return orderfilled_execution.target_notional(params)


def _fill_decision(
    params: Any,
    point: Any,
    run: dict[str, Any],
    side: str,
    *,
    target_size: Decimal | None = None,
) -> dict[str, Any]:
    price = Decimal(str(point.price))
    mode = str(getattr(params, "execution_price_mode", "ORDERFILLED_CROSS") or "ORDERFILLED_CROSS").strip().upper().replace("-", "_")
    if mode == "ORDERFILLED":
        return _orderfilled_fill_decision(params, point, side, target_size=target_size)
    if mode in ORDERFILLED_CROSS_MODES:
        return _orderfilled_fill_decision(params, point, side, target_size=target_size)
    if mode in ORDERFILLED_LOB_MODES:
        orderfilled_fill = _orderfilled_fill_decision(params, point, side, target_size=target_size)
        l2_fill = _depth_fill_decision(params, point, run, side, target_size=orderfilled_fill.get("requested_size") or target_size)
        return combine_orderfilled_l2_execution(orderfilled_fill, l2_fill, signal_price=price, side=side)
    return _depth_fill_decision(params, point, run, side, target_size=target_size)


def _depth_fill_decision(
    params: Any,
    point: Any,
    run: dict[str, Any],
    side: str,
    *,
    target_size: Decimal | Any | None = None,
):
    price = Decimal(str(point.price))
    provider = run.get("_pmxt_compact_book_provider")
    if provider is not None and getattr(point, "timestamp", None) is not None:
        config = l2_config_from_params(params)
        execution_ts = point.timestamp + timedelta(milliseconds=config.submit_latency_ms)
        snapshot = provider.snapshot_at(execution_ts)
        snapshots = [snapshot] if snapshot is not None else []
    else:
        snapshots = [
            snapshot
            for snapshot in (snapshot_from_any(item) for item in run.get("_clob_snapshots") or [])
            if snapshot is not None
        ]
    requested_size = Decimal(str(target_size)) if target_size is not None else None
    if requested_size is None:
        target_notional = _target_notional(params)
        requested_size = (target_notional / max(price, Decimal("0.0000000001"))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    return simulate_l2_depth_execution(
        snapshots=snapshots,
        decision_block=int(point.x_value) if run.get("price_source") == "orderfilled_block_close" else None,
        decision_timestamp=getattr(point, "timestamp", None),
        side=side,  # type: ignore[arg-type]
        target_size=max(Decimal("0"), requested_size),
        signal_price=price,
        params=params,
        market_id=str(run.get("market_id") or run.get("market_slug") or ""),
        asset_id=str((run.get("meta") or {}).get("token_id") if isinstance(run.get("meta"), dict) else ""),
    )


def _orderfilled_fill_decision(
    params: Any,
    point: Any,
    side: str,
    *,
    target_size: Decimal | None = None,
) -> dict[str, Any]:
    return orderfilled_execution.orderfilled_fill_decision(params, point, side, target_size=target_size)
