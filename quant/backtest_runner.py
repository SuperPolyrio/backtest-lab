"""Compatibility wrapper for quant.workers.backtest_runner."""

from .workers.backtest_runner import *  # noqa: F401,F403

if __name__ == "__main__":
    from .workers.backtest_runner import main

    main()
