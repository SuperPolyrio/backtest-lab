"""CLI entrypoint for all public backtest validation runners."""

from __future__ import annotations

from .public import main


if __name__ == "__main__":
    raise SystemExit(main())
