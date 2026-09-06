from __future__ import annotations

import pytest

from quant.backtest.backtest_engine import _bounded_price_points


def test_backtest_price_window_limit_fails_instead_of_silently_truncating() -> None:
    with pytest.raises(RuntimeError, match="POLYDATA_QUANT_BACKTEST_MAX_PRICE_POINTS=2"):
        _bounded_price_points(
            [{"x_value": 1}, {"x_value": 2}, {"x_value": 3}],
            limit=2,
            run={
                "price_source": "orderfilled_block_close",
                "from_block": 1,
                "to_block": 3,
            },
        )
