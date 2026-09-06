"""Python 3.12 worker for running NautilusTrader backtests.

The main polymonitor runtime can stay on Python 3.10. This script is launched
with a dedicated Python 3.12 environment that has `nautilus_trader` installed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.backtest_engine import BacktestParameters, PricePoint, build_metrics  # noqa: E402
from quant.backtest.frameworks import _run_nautilus_trader  # noqa: E402


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: nautilus_worker.py INPUT_JSON OUTPUT_JSON")
    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    points = [
        PricePoint(
            x_value=int(row["x_value"]),
            price=Decimal(str(row["price"])),
            volume=Decimal(str(row.get("volume") or "0")),
            trade_count=int(row.get("trade_count") or 0),
            timestamp=_datetime_or_none(row.get("timestamp")),
        )
        for row in payload["points"]
    ]
    params = BacktestParameters(
        entry_threshold=Decimal(str(payload["params"]["entry_threshold"])),
        exit_threshold=Decimal(str(payload["params"]["exit_threshold"])),
        stop_loss=Decimal(str(payload["params"]["stop_loss"])),
        take_profit=Decimal(str(payload["params"]["take_profit"])),
        max_holding_bars=int(payload["params"]["max_holding_bars"]),
        initial_capital=Decimal(str(payload["params"]["initial_capital"])),
        position_size=Decimal(str(payload["params"]["position_size"])),
        fee_bps=Decimal(str(payload["params"].get("fee_bps", "0"))),
        slippage_bps=Decimal(str(payload["params"].get("slippage_bps", "0"))),
        liquidity_cap_pct=Decimal(str(payload["params"].get("liquidity_cap_pct", "100"))),
        max_position_notional=Decimal(str(payload["params"].get("max_position_notional", "0"))),
        min_fill_pct=Decimal(str(payload["params"].get("min_fill_pct", "0"))),
        execution_price_mode=str(payload["params"].get("execution_price_mode", "ORDERFILLED_CROSS")),
        latency_seconds=Decimal(str(payload["params"].get("latency_seconds", "0"))),
        max_book_staleness_seconds=Decimal(str(payload["params"].get("max_book_staleness_seconds", "900"))),
        allow_partial_fill=bool(payload["params"].get("allow_partial_fill", True)),
        min_fill_size=Decimal(str(payload["params"].get("min_fill_size", "0"))),
        reject_on_stale_book=bool(payload["params"].get("reject_on_stale_book", True)),
        final_valuation_mode=str(payload["params"].get("final_valuation_mode", "SETTLEMENT")),
        buy_limit_price=Decimal(str(payload["params"]["buy_limit_price"])) if str(payload["params"].get("buy_limit_price", "")).strip() else None,
        sell_limit_price=Decimal(str(payload["params"]["sell_limit_price"])) if str(payload["params"].get("sell_limit_price", "")).strip() else None,
        settlement_value=Decimal(str(payload["params"]["settlement_value"])) if str(payload["params"].get("settlement_value", "")).strip() else None,
        max_entry_price=Decimal(str(payload["params"].get("max_entry_price", "1"))),
        min_exit_price=Decimal(str(payload["params"].get("min_exit_price", "0"))),
    )
    result = _run_nautilus_trader(points, dict(payload["run"]), params, build_metrics)
    output_path.write_text(json.dumps(_json_ready(result)), encoding="utf-8")


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    main()
