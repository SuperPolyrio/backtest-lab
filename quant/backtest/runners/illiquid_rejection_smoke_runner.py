from __future__ import annotations

from .base import print_results
from .public import IlliquidRejectionSmokeRunner


if __name__ == "__main__":
    result = IlliquidRejectionSmokeRunner().run()
    print_results([result], json_output=True)
    raise SystemExit(0 if result.passed else 1)
