"""Test existing Paper against the extracted backtest package, without cutting over services."""

import argparse
import importlib.util
import os
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--junitxml", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    target = Path(__file__).resolve().parents[1]
    os.environ["POLY_QUANT_DISABLE_DOTENV"] = "1"
    os.chdir(source)
    sys.path.insert(0, str(source))
    import quant

    assert Path(quant.__file__).resolve().is_relative_to(source)
    location = target / "quant/backtest"
    spec = importlib.util.spec_from_file_location(
        "quant.backtest", location / "__init__.py", submodule_search_locations=[str(location)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["quant.backtest"] = module
    quant.backtest = module
    spec.loader.exec_module(module)

    import pytest

    class OfflineGuard:
        @pytest.fixture(autouse=True)
        def prohibit_network(self, monkeypatch):
            def denied(*_args, **_kwargs):
                raise AssertionError("Network access is forbidden in the migration Paper regression")
            monkeypatch.setattr("socket.socket.connect", denied)
            monkeypatch.setattr("socket.create_connection", denied)

    paths = [
        "quant/backtest/tests/test_paper_ledger.py",
        "quant/backtest/tests/test_paper_taker_execution.py",
        "quant/backtest/tests/test_paper_mock_execution_engine.py",
        "quant/backtest/tests/test_paper_paired_probe.py",
        "tests/execution/test_paper_public_api.py",
        "tests/execution/test_paper_tenant_platform.py",
        "tests/calibration/test_paired_probe_bridge.py",
    ]
    code = pytest.main([str(source / p) for p in paths] + [
        "-q", "--tb=short", "--disable-warnings", "--import-mode=importlib",
        "--basetemp=/tmp/backtest-lab-paper-compatibility", f"--junitxml={args.junitxml}",
    ], plugins=[OfflineGuard()])
    from quant.backtest import order_event_collector, l2_orderfilled_execution

    for loaded in (order_event_collector, l2_orderfilled_execution):
        assert Path(loaded.__file__).resolve().is_relative_to(target), loaded.__file__
    raise SystemExit(code)


if __name__ == "__main__":
    main()
