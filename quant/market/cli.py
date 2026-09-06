"""CLI for operating and inspecting the dynamic paper Market Registry."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant.core.db import env_first, postgres_connection
from quant.market.api_client import MarketRegistryApiConfig, PolymarketApiClient
from quant.market.registry_lifecycle_replay import load_lifecycle_jsonl, replay_lifecycle_events
from quant.market.repository import MarketRegistryRepository
from quant.market.service import MarketRegistryService
from quant.market.registry_soak_report import build_soak_report, load_soak_samples
from quant.market.token_universe import MarketUniverseConfig, compute_universe_decisions


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "soak-report":
        report = build_soak_report(load_soak_samples(args.jsonl), failure_threshold=args.failure_threshold)
        text = _json(report.as_dict())
        print(text)
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(text + "\n", encoding="utf-8")
        return 0 if report.status != "FAIL" else 1
    if args.command == "lifecycle-replay":
        report = replay_lifecycle_events(load_lifecycle_jsonl(args.jsonl), max_issues=args.max_issues)
        text = _json(report.as_dict())
        print(text)
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(text + "\n", encoding="utf-8")
        return 0 if report.status == "PASS" else 1
    config = MarketUniverseConfig(
        book_ttl_seconds=args.book_ttl_seconds,
        min_market_token_count=args.min_market_token_count,
        require_status_snapshot=not args.allow_status_missing,
        exclude_placeholders=not args.allow_placeholders,
    )
    readonly = args.command in {"registry-status", "health", "print-subscription-universe", "print-execution-universe", "print-market", "print-token"}
    with postgres_connection(readonly=readonly) as conn:
        repo = MarketRegistryRepository(conn)
        service = MarketRegistryService(conn, config=config, api_client=_api_client(args))
        if args.command == "full-sync":
            _maybe_init_schema(service, args)
            print(_json(asdict(service.full_sync(limit=args.limit, include_api=not args.no_api, api_limit=args.api_limit, api_page_size=args.api_page_size))))
            return 0
        if args.command == "delta-poll":
            _maybe_init_schema(service, args)
            print(_json(asdict(service.delta_poll(limit=args.limit, include_api=not args.no_api, api_limit=args.api_limit, api_page_size=args.api_page_size))))
            return 0
        if args.command == "probe-pending":
            _maybe_init_schema(service, args)
            probe_limit = args.limit if int(args.limit) > 0 and int(args.probe_limit) == 100 else args.probe_limit
            print(_json(asdict(service.probe_pending_books(limit=probe_limit, batch_size=args.probe_batch_size))))
            return 0
        if args.command == "registry-status":
            print(_json(repo.status()))
            return 0
        if args.command == "health":
            print(_json(repo.status()))
            return 0
        if args.command == "print-subscription-universe":
            print(_json(_read_universe(conn, "quant.paper_active_market_registry_tokens", limit=args.limit)))
            return 0
        if args.command == "print-execution-universe":
            print(_json(_read_universe(conn, "quant.paper_execution_market_registry_tokens", limit=args.limit)))
            return 0
        if args.command == "print-market":
            print(_json(_read_market(conn, args)))
            return 0
        if args.command == "print-token":
            rows = _read_token(conn, args.asset_id)
            print(_json(rows))
            return 0 if rows else 1
        if args.command == "force-recheck-token":
            print(_json(_force_recheck_assets(service, [args.asset_id])))
            return 0
        if args.command == "force-recheck-market":
            assets = _market_assets(conn, args)
            print(_json(_force_recheck_assets(service, assets)))
            return 0
        if args.command == "apply-lob-status":
            payload = json.loads(args.payload_json)
            print(_json(service.handle_lob_collector_status(payload)))
            return 0
        if args.command == "republish-subscriptions":
            print(_json(service.republish_desired_subscriptions(reason="cli")))
            return 0
        if args.command == "publish-outbox":
            _maybe_init_schema(service, args)
            print(_json(service.publish_pending_outbox(limit=args.outbox_publish_limit).as_meta()))
            return 0
        if args.command == "repair-registry":
            _maybe_init_schema(service, args)
            print(_json(service.repair_registry_state().as_meta()))
            return 0
    raise SystemExit(f"unknown command: {args.command}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--limit", type=int, default=0, help="limit rows; 0 means no limit where supported")
    common.add_argument("--api-limit", type=int, default=0)
    common.add_argument("--api-page-size", type=int, default=100)
    common.add_argument("--probe-limit", type=int, default=100)
    common.add_argument("--probe-batch-size", type=int, default=500)
    common.add_argument("--outbox-publish-limit", type=int, default=5_000)
    common.add_argument("--book-ttl-seconds", type=int, default=900)
    common.add_argument("--min-market-token-count", type=int, default=2)
    common.add_argument("--allow-status-missing", action="store_true")
    common.add_argument("--allow-placeholders", action="store_true")
    common.add_argument("--no-api", action="store_true")
    common.add_argument(
        "--skip-init-schema",
        action="store_true",
        help="Use the existing schema without running DDL; recommended for live maintenance.",
    )
    common.add_argument("--gamma-api-base", default=env_first("POLYDATA_GAMMA_API_BASE", default="https://gamma-api.polymarket.com"))
    common.add_argument("--clob-api-base", default=env_first("POLYDATA_CLOB_API_BASE", default="https://clob.polymarket.com"))
    common.add_argument("--api-timeout-seconds", type=float, default=float(env_first("POLYDATA_MARKET_REGISTRY_API_TIMEOUT_SECONDS", default="15")))
    common.add_argument("--proxy-mode", default=env_first("POLYDATA_MARKET_REGISTRY_PROXY_MODE", default="direct"), choices=("direct", "env", "explicit"))
    common.add_argument("--proxy-url", default=env_first("POLYDATA_MARKET_REGISTRY_PROXY_URL", default=""))
    sub = parser.add_subparsers(dest="command", required=True)
    full_sync = sub.add_parser("full-sync", parents=[common])
    full_sync.add_argument("--active-only", action="store_true", help="Accepted for runbook compatibility; active-only is the default.")
    delta_poll = sub.add_parser("delta-poll", parents=[common])
    delta_poll.add_argument("--once", action="store_true", help="Accepted for runbook compatibility; this command already runs once.")
    sub.add_parser("probe-pending", parents=[common])
    sub.add_parser("registry-status", parents=[common])
    sub.add_parser("health", parents=[common])
    sub.add_parser("print-subscription-universe", parents=[common])
    sub.add_parser("print-execution-universe", parents=[common])
    market = sub.add_parser("print-market", parents=[common])
    market.add_argument("--market-id")
    market.add_argument("--gamma-market-id")
    market.add_argument("--condition-id")
    market.add_argument("--market-slug")
    token = sub.add_parser("print-token", parents=[common])
    token.add_argument("--asset-id", required=True)
    recheck_token = sub.add_parser("force-recheck-token", parents=[common])
    recheck_token.add_argument("--asset-id", required=True)
    recheck_market = sub.add_parser("force-recheck-market", parents=[common])
    recheck_market.add_argument("--market-id")
    recheck_market.add_argument("--gamma-market-id")
    recheck_market.add_argument("--condition-id")
    recheck_market.add_argument("--market-slug")
    lob = sub.add_parser("apply-lob-status", parents=[common])
    lob.add_argument("--payload-json", required=True)
    sub.add_parser("republish-subscriptions", parents=[common])
    sub.add_parser("publish-outbox", parents=[common])
    sub.add_parser("repair-registry", parents=[common])
    soak = sub.add_parser("soak-report")
    soak.add_argument("--jsonl", required=True, type=Path)
    soak.add_argument("--failure-threshold", type=int, default=3)
    soak.add_argument("--json-out", type=Path, default=None)
    replay = sub.add_parser("lifecycle-replay")
    replay.add_argument("--jsonl", required=True, type=Path)
    replay.add_argument("--json-out", type=Path, default=None)
    replay.add_argument("--max-issues", type=int, default=100)
    return parser


def _api_client(args: argparse.Namespace) -> PolymarketApiClient | None:
    if getattr(args, "no_api", False):
        return None
    return PolymarketApiClient(
        MarketRegistryApiConfig(
            gamma_api_base=args.gamma_api_base,
            clob_api_base=args.clob_api_base,
            timeout_seconds=float(args.api_timeout_seconds),
            proxy_mode=args.proxy_mode,
            proxy_url=args.proxy_url,
            trust_env_proxy=args.proxy_mode == "env",
        )
    )


def _maybe_init_schema(service: MarketRegistryService, args: argparse.Namespace) -> None:
    if not bool(getattr(args, "skip_init_schema", False)):
        service.init_schema()


def _read_universe(conn: Any, view: str, *, limit: int) -> list[dict[str, Any]]:
    limit_sql = "" if int(limit) <= 0 else "LIMIT %s"
    params: tuple[Any, ...] = () if int(limit) <= 0 else (int(limit),)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT asset_id, market_id, gamma_market_id, condition_id, market_slug,
                   outcome_name, market_state, desired_subscribed, actual_subscribed,
                   best_bid, best_ask, book_quality, book_age_ms
            FROM {view}
            ORDER BY updated_at DESC
            {limit_sql}
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _read_market(conn: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if args.market_id:
        clauses.append("market_id = %s")
        params.append(int(args.market_id))
    if args.gamma_market_id:
        clauses.append("gamma_market_id = %s")
        params.append(str(args.gamma_market_id))
    if args.condition_id:
        clauses.append("condition_id = %s")
        params.append(str(args.condition_id))
    if args.market_slug:
        clauses.append("market_slug = %s")
        params.append(str(args.market_slug))
    if not clauses:
        raise SystemExit("one of --market-id/--gamma-market-id/--condition-id/--market-slug is required")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.paper_market_registry_tokens
            WHERE {' OR '.join(clauses)}
            ORDER BY outcome_index NULLS LAST, asset_id
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _read_token(conn: Any, asset_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.paper_market_registry_tokens WHERE asset_id = %s", (str(asset_id),))
        return [dict(row) for row in cur.fetchall()]


def _market_assets(conn: Any, args: argparse.Namespace) -> list[str]:
    return [str(row["asset_id"]) for row in _read_market(conn, args)]


def _force_recheck_assets(service: MarketRegistryService, asset_ids: list[str]) -> dict[str, Any]:
    if service.api_client is None:
        raise RuntimeError("force recheck requires API client")
    run_id = service.repo.begin_sync_run("force_recheck", meta={"asset_count": len(asset_ids)})
    probes = service.api_client.probe_books(asset_ids, batch_size=500)
    try:
        tokens = service.repo.fetch_state_tokens(asset_ids)
        by_asset = {token.asset_id: token for token in tokens}
        decisions = []
        for asset_id, probe in probes.items():
            token = by_asset.get(asset_id)
            if token is None:
                continue
            decision = compute_universe_decisions([probe.apply_to_token(token)], config=service.config)[0]
            service.repo.update_probe_result(decision, probe, run_id=run_id)
            decisions.append(decision)
        # A targeted recheck must remain targeted. Recomputing every known
        # token here made a single CLI probe scan and rewrite the full registry.
        summary = service.repo.persist_partial_decisions(
            decisions,
            run_id=run_id,
            source="force_recheck",
            repair_global_targets=False,
            include_universe_counts=False,
        )
        service.repo.finish_sync_run(run_id, status="success", summary=summary, meta={"probes_ok": sum(1 for probe in probes.values() if probe.ok)})
        return {"run_id": run_id, "assets": len(asset_ids), "probes_ok": sum(1 for probe in probes.values() if probe.ok)}
    except Exception as exc:
        service._rollback_after_error()
        service.repo.finish_sync_run(run_id, status="error", error=str(exc))
        raise


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
