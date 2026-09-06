from __future__ import annotations

from .base import print_results
from .public import NoTradeReplaySmokeRunner


if __name__ == "__main__":
    result = NoTradeReplaySmokeRunner().run()
    print_results([result], json_output=True)
    raise SystemExit(0 if result.passed else 1)
