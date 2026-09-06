from __future__ import annotations

from .base import print_results
from .public import LimitOrderLifecycleTest


if __name__ == "__main__":
    result = LimitOrderLifecycleTest().run()
    print_results([result], json_output=True)
    raise SystemExit(0 if result.passed else 1)
