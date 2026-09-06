"""CLI entrypoint for account ledger replay validation."""

from __future__ import annotations

import argparse
import os

from .base import print_results
from .public import AccountLedgerReplayRunner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run account ledger replay validation.")
    parser.add_argument("--mode", choices=("fixture", "db"), default="fixture")
    parser.add_argument("--address", default=os.environ.get("POLYDATA_VALIDATION_ADDRESS", ""))
    parser.add_argument("--market-slug", default=os.environ.get("POLYDATA_VALIDATION_MARKET_SLUG", ""))
    parser.add_argument("--token-side", default=os.environ.get("POLYDATA_VALIDATION_TOKEN_SIDE", "YES"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    # The fixture runner ignores market arguments; keep the flags for CLI
    # compatibility with DB mode and future raw account-cashflow replay.
    _ = (args.market_slug, args.token_side)
    result = AccountLedgerReplayRunner().run(mode=args.mode, address=args.address or None)
    print_results([result], json_output=args.json)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
