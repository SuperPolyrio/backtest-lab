#!/usr/bin/env python3
"""Plan or record a fill-first guarded strategy activation decision."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.run_artifacts import (  # noqa: E402
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
)
from quant.backtest.strategy_activation import (  # noqa: E402
    build_strategy_activation_decision,
    build_strategy_enable_state,
    insert_strategy_activation_decision,
    strategy_activation_decision_to_markdown,
    strategy_enable_state_to_markdown,
    upsert_strategy_enable_state,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id. Defaults to latest fill-first run.")
    parser.add_argument("--target-mode", choices=("backtest", "paper", "live"), default="paper")
    parser.add_argument("--requested-by", default="", help="Operator/user requesting the activation decision.")
    parser.add_argument("--notes", default="", help="Optional review note stored with --write.")
    parser.add_argument("--write", action="store_true", help="Persist the decision to quant.strategy_activation_decisions.")
    parser.add_argument("--set-enable", choices=("none", "enabled", "disabled"), default="none", help="Optionally update quant.strategy_enable_state from the written activation decision.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when activation is not allowed.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=not args.write) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id) if run_id is not None else None
        report = build_backtest_run_artifact_report(inputs, run_id=run_id)
        decision = build_strategy_activation_decision(
            report,
            target_mode=args.target_mode,
            requested_by=args.requested_by,
            notes=args.notes,
        )
        if args.write:
            decision = insert_strategy_activation_decision(conn, decision)
        enable_state = None
        if args.set_enable != "none":
            if not args.write:
                raise SystemExit("--set-enable requires --write so the enable state references a persisted decision_id")
            enable_state = build_strategy_enable_state(
                decision,
                enable=args.set_enable == "enabled",
                requested_by=args.requested_by,
                reason=args.notes,
            )
            enable_state = upsert_strategy_enable_state(conn, enable_state)

    if args.format == "json":
        payload = {"decision": decision}
        if enable_state is not None:
            payload["enable_state"] = enable_state
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        if args.write:
            print(f"recorded_decision_id: {decision.get('decision_id')}")
            print("")
        print(strategy_activation_decision_to_markdown(decision))
        if enable_state is not None:
            print("")
            print(strategy_enable_state_to_markdown(enable_state))
    return 0 if decision.get("activation_allowed") or not args.strict else 1


if __name__ == "__main__":
    raise SystemExit(main())
