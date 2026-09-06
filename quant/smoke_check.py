"""Local smoke checks for the quant package.

The default check is intentionally fixture-only so it can run on a developer
machine without database or network access.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from decimal import Decimal
from typing import Any

from .backtest.runners.public import run_all_public
from .core.db import ClickHouseClient, database_settings_summary, postgres_connection


def run_smoke(*, include_db: bool = False) -> dict[str, Any]:
    results = run_all_public(mode="fixture")
    db_health = run_db_health_check() if include_db else None
    return {
        "mode": "fixture+db_health" if include_db else "fixture",
        "database_settings": database_settings_summary(),
        "passed": all(result.passed for result in results) and (not db_health or db_health["passed"]),
        "results": [asdict(result) for result in results],
        "db_health": db_health,
    }


def run_db_health_check() -> dict[str, Any]:
    report: dict[str, Any] = {
        "passed": False,
        "postgres": {},
        "clickhouse": {},
        "sample_market": None,
    }
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'quant') AS exists")
            report["postgres"]["quant_schema_exists"] = bool(cur.fetchone()["exists"])
            cur.execute("SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = 'quant'")
            report["postgres"]["quant_table_count"] = int(cur.fetchone()["n"])
            for table in (
                "quant.market_token_metadata",
                "quant.market_price_eligibility",
                "quant.market_token_frontend_price_1m",
                "quant.market_token_block_close",
                "quant.quant_backtest_runs",
            ):
                cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
                report["postgres"][table.rsplit(".", 1)[-1]] = int(cur.fetchone()["n"])
            cur.execute(
                """
                SELECT
                    market_id,
                    market_slug,
                    token_id,
                    token_side,
                    COUNT(*) AS rows,
                    MIN(block_number) AS from_block,
                    MAX(block_number) AS to_block,
                    MIN(close_price) AS min_price,
                    MAX(close_price) AS max_price
                FROM quant.market_token_block_close
                GROUP BY market_id, market_slug, token_id, token_side
                HAVING COUNT(*) >= 3
                ORDER BY COUNT(*) DESC
                LIMIT 1
                """
            )
            sample = cur.fetchone()
            if sample:
                cur.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM (
                        SELECT block_number, close_price
                        FROM quant.market_token_block_close
                        WHERE token_id = %s
                        ORDER BY block_number ASC
                        LIMIT 100
                    ) sample_bars
                    """,
                    (sample["token_id"],),
                )
                sample_rows = int(cur.fetchone()["n"])
                report["sample_market"] = {
                    "market_id": int(sample["market_id"]),
                    "market_slug": str(sample["market_slug"]),
                    "token_id": str(sample["token_id"]),
                    "token_side": str(sample["token_side"]),
                    "rows": int(sample["rows"]),
                    "from_block": int(sample["from_block"]),
                    "to_block": int(sample["to_block"]),
                    "sample_bars_loaded": sample_rows,
                    "min_price": str(Decimal(str(sample["min_price"]))),
                    "max_price": str(Decimal(str(sample["max_price"]))),
                }
    clickhouse_sample = ClickHouseClient().query_scalar("SELECT 1 FROM orderfilled_fact LIMIT 1", timeout_seconds=20)
    report["clickhouse"]["orderfilled_fact_readable"] = clickhouse_sample.strip() == "1"
    report["passed"] = bool(
        report["postgres"].get("quant_schema_exists")
        and report["postgres"].get("quant_table_count", 0) > 0
        and report["sample_market"]
        and report["sample_market"]["sample_bars_loaded"] > 0
        and report["clickhouse"].get("orderfilled_fact_readable")
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run local quant smoke checks.")
    parser.add_argument("--db", action="store_true", help="Also verify Postgres/ClickHouse connectivity and sample quant price data.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    report = run_smoke(include_db=args.db)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, default=str, sort_keys=True))
    else:
        print(f"mode: {report['mode']}")
        print(f"passed: {report['passed']}")
        print(f"postgres: {report['database_settings']['postgres']}")
        print(f"clickhouse: {report['database_settings']['clickhouse']}")
        for result in report["results"]:
            status = "PASS" if result["passed"] else "FAIL"
            print(f"{status} {result['run_id']}: rows={result['rows_scanned']} bars={result['bars_processed']}")
            if result["message"]:
                print(f"  {result['message']}")
        if report["db_health"]:
            print(f"db_health: {report['db_health']['passed']}")
            print(f"sample_market: {report['db_health']['sample_market']}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
