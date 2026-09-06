"""Dynamic paper Market Registry daemon.

The daemon derives a tradable token universe from the existing database and
Polymarket APIs, persists state transitions, and publishes LOB subscription
requests through a DB outbox.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import sys
import threading
import time
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Literal

from quant.core.db import env_bool, env_first, postgres_connection

from .api_client import MarketRegistryApiConfig, PolymarketApiClient
from .repository import MarketRegistryRepository
from .service import MarketRegistryService
from .token_universe import MarketUniverseConfig


DEFAULT_WS_URL = env_first("POLYDATA_CLOB_WS_URL", default="wss://ws-subscriptions-clob.polymarket.com/ws/market")


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        argv = ["run"]
    args = build_parser().parse_args(argv)
    config = MarketUniverseConfig(
        book_ttl_seconds=args.book_ttl_seconds,
        min_market_token_count=args.min_market_token_count,
        require_status_snapshot=not args.allow_status_missing,
        exclude_placeholders=not args.allow_placeholders,
    )
    readonly = args.command in {"registry-status", "health", "print-outbox"}
    with _api_context(args) as api_client:
        with postgres_connection(readonly=readonly) as conn:
            service = MarketRegistryService(conn, config=config, api_client=api_client)
            repo = MarketRegistryRepository(conn)
            if args.command == "init-schema":
                service.init_schema()
                print(_json({"status": "ok", "schema": "quant.paper_market_registry"}))
                return 0
            if args.command == "full-sync":
                _init_schema_if_needed(service, args)
                summary = service.full_sync(
                    limit=args.limit,
                    include_api=not args.no_api,
                    api_limit=args.api_limit,
                    api_page_size=args.api_page_size,
                )
                print(_json(asdict(summary)))
                return 0
            if args.command == "delta-poll":
                _init_schema_if_needed(service, args)
                summary = service.delta_poll(
                    limit=args.limit,
                    include_api=not args.no_api,
                    api_limit=args.api_limit,
                    api_page_size=args.api_page_size,
                )
                print(_json(asdict(summary)))
                return 0
            if args.command == "probe-pending":
                _init_schema_if_needed(service, args)
                summary = service.probe_pending_books(limit=args.probe_limit, batch_size=args.probe_batch_size)
                print(_json(asdict(summary)))
                return 0
            if args.command == "recheck-stale-markets":
                _init_schema_if_needed(service, args)
                summary = service.recheck_stale_markets(
                    limit=args.stale_recheck_limit,
                    source_age_seconds=args.stale_recheck_source_age_seconds,
                    retry_seconds=args.stale_recheck_retry_seconds,
                    probe_batch_size=args.probe_batch_size,
                    concurrency=args.stale_recheck_concurrency,
                )
                print(_json(asdict(summary)))
                return 0
            if args.command == "publish-outbox":
                _init_schema_if_needed(service, args)
                publish_summary = service.publish_pending_outbox(limit=args.outbox_publish_limit)
                print(_json(publish_summary.as_meta()))
                return 0
            if args.command == "repair-registry":
                _init_schema_if_needed(service, args)
                repair_summary = service.repair_registry_state()
                print(_json(repair_summary.as_meta()))
                return 0
            if args.command == "refresh-lob-projection":
                _init_schema_if_needed(service, args)
                print(_json(service.refresh_lob_projection(ttl_seconds=args.lob_projection_ttl_seconds)))
                return 0
            if args.command == "clob-markets-refresh":
                _init_schema_if_needed(service, args)
                summary = service.clob_markets_refresh(
                    page_budget=args.clob_markets_page_budget,
                    max_open_markets=args.clob_markets_max_open_markets,
                    rescan_seconds=args.clob_markets_rescan_seconds,
                    force_restart=args.clob_markets_force_restart,
                )
                print(_json(asdict(summary)))
                return 0
            if args.command == "run":
                _init_schema_if_needed(service, args)
                with _embedded_ws_lifecycle_worker(args, config) as ws_worker:
                    with _embedded_registry_heartbeat_worker(args, ws_worker):
                        with _embedded_delta_poll_worker(args, config) as delta_worker:
                            probe_limit = _bounded_cycle_limit(
                                args.probe_limit,
                                args.probe_cycle_limit,
                            )
                            service.run_loop(
                                poll_seconds=-1.0 if delta_worker is not None else args.poll_seconds,
                                probe_seconds=args.probe_seconds,
                                full_sync_seconds=args.full_sync_seconds,
                                limit=args.limit,
                                probe_limit=probe_limit,
                                api_limit=args.api_limit,
                                api_page_size=args.api_page_size,
                                probe_batch_size=args.probe_batch_size,
                                outbox_publish_seconds=args.outbox_publish_seconds,
                                outbox_publish_limit=args.outbox_publish_limit,
                                api_delta_seconds=args.api_delta_seconds,
                                api_delta_limit=args.api_delta_limit,
                                clob_markets_seconds=args.clob_markets_seconds,
                                clob_markets_page_budget=args.clob_markets_page_budget,
                                clob_markets_rescan_seconds=args.clob_markets_rescan_seconds,
                                stale_recheck_seconds=args.stale_recheck_seconds,
                                stale_recheck_limit=args.stale_recheck_limit,
                                stale_recheck_source_age_seconds=args.stale_recheck_source_age_seconds,
                                stale_recheck_retry_seconds=args.stale_recheck_retry_seconds,
                                stale_recheck_concurrency=args.stale_recheck_concurrency,
                                lob_projection_seconds=args.lob_projection_seconds,
                                skip_startup_probe=args.skip_startup_probe,
                                skip_startup_lob_projection=args.skip_startup_lob_projection,
                                skip_startup_republish=args.skip_startup_republish,
                                include_api=not args.no_api,
                                periodic_full_include_api=(
                                    args.periodic_full_with_api and not args.no_api
                                ),
                                run_seconds=args.run_seconds,
                                once=args.once,
                                allow_start_without_full_sync=args.allow_start_without_full_sync,
                                skip_startup_full_sync=args.skip_startup_full_sync,
                                ws_connected_provider=ws_worker.is_connected if ws_worker is not None else None,
                            )
                print(_json({"status": "stopped"}))
                return 0
            if args.command == "ws-lifecycle":
                _init_schema_if_needed(service, args)
                asyncio.run(run_ws_lifecycle_loop(service, repo, args))
                return 0
            if args.command == "registry-status":
                print(_json(repo.status()))
                return 0
            if args.command == "health":
                print(_json(repo.status()))
                return 0
            if args.command == "print-outbox":
                print(_json(_read_outbox(repo, limit=args.limit)))
                return 0
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--limit", type=int, default=0, help="Max DB/source tokens to read; 0 means full universe.")
    common.add_argument("--api-limit", type=int, default=0, help="Max Gamma markets to read; 0 means all active markets.")
    common.add_argument("--api-page-size", type=int, default=500)
    common.add_argument("--probe-limit", type=int, default=0, help="Max pending books to probe; 0 means all pending candidates.")
    common.add_argument("--probe-batch-size", type=int, default=50)
    common.add_argument("--outbox-publish-limit", type=int, default=5_000, help="Max pending outbox rows to publish per cycle; 0 means all.")
    common.add_argument("--book-ttl-seconds", type=int, default=900)
    common.add_argument("--min-market-token-count", type=int, default=2)
    common.add_argument("--allow-status-missing", action="store_true")
    common.add_argument("--allow-placeholders", action="store_true")
    common.add_argument("--no-api", action="store_true", help="Do not call Gamma/CLOB APIs.")
    common.add_argument("--gamma-api-base", default=env_first("POLYDATA_GAMMA_API_BASE", default="https://gamma-api.polymarket.com"))
    common.add_argument("--clob-api-base", default=env_first("POLYDATA_CLOB_API_BASE", default="https://clob.polymarket.com"))
    common.add_argument("--api-timeout-seconds", type=float, default=float(env_first("POLYDATA_MARKET_REGISTRY_API_TIMEOUT_SECONDS", default="15")))
    common.add_argument("--proxy-mode", default=env_first("POLYDATA_MARKET_REGISTRY_PROXY_MODE", default="direct"), choices=("direct", "env", "explicit"))
    common.add_argument("--proxy-url", default=env_first("POLYDATA_MARKET_REGISTRY_PROXY_URL", default=""))
    common.add_argument(
        "--fallback-proxy-urls",
        default=env_first("POLYDATA_MARKET_REGISTRY_FALLBACK_PROXY_URLS", default=""),
    )
    common.add_argument("--trust-env-proxy", action="store_true")
    common.add_argument("--skip-init-schema", action="store_true", help="Assume schema exists and skip startup schema migration/lock.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-schema", parents=[common])
    sub.add_parser("full-sync", parents=[common])
    sub.add_parser("delta-poll", parents=[common])
    sub.add_parser("probe-pending", parents=[common])
    stale_recheck = sub.add_parser("recheck-stale-markets", parents=[common])
    stale_recheck.add_argument(
        "--stale-recheck-limit",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_LIMIT", default="100")),
    )
    stale_recheck.add_argument(
        "--stale-recheck-source-age-seconds",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_SOURCE_AGE_SECONDS", default="604800")),
    )
    stale_recheck.add_argument(
        "--stale-recheck-retry-seconds",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_RETRY_SECONDS", default="21600")),
    )
    stale_recheck.add_argument(
        "--stale-recheck-concurrency",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_CONCURRENCY", default="4")),
    )
    sub.add_parser("publish-outbox", parents=[common])
    sub.add_parser("repair-registry", parents=[common])
    refresh = sub.add_parser("refresh-lob-projection", parents=[common])
    refresh.add_argument("--lob-projection-ttl-seconds", type=int, default=int(env_first("REGISTRY_LOB_PROJECTION_TTL_SECONDS", default="300")))
    clob_refresh = sub.add_parser("clob-markets-refresh", parents=[common])
    clob_refresh.add_argument("--clob-markets-page-budget", type=int, default=int(env_first("REGISTRY_CLOB_MARKETS_PAGE_BUDGET", default="10")))
    clob_refresh.add_argument("--clob-markets-max-open-markets", type=int, default=0)
    clob_refresh.add_argument("--clob-markets-rescan-seconds", type=float, default=float(env_first("REGISTRY_CLOB_MARKETS_RESCAN_SECONDS", default="1800")))
    clob_refresh.add_argument("--clob-markets-force-restart", action="store_true")
    run = sub.add_parser("run", parents=[common])
    run.add_argument("--poll-seconds", type=float, default=float(env_first("REGISTRY_DELTA_POLL_INTERVAL_SECONDS", default="30")))
    run.add_argument("--probe-seconds", type=float, default=float(env_first("REGISTRY_BOOK_PROBE_INTERVAL_SECONDS", default="10")))
    run.add_argument(
        "--probe-cycle-limit",
        type=int,
        default=int(env_first("REGISTRY_BOOK_PROBE_CYCLE_LIMIT", default="100")),
        help="Fairness cap for one daemon probe cycle; 0 disables the cap.",
    )
    run.add_argument("--full-sync-seconds", type=float, default=float(env_first("REGISTRY_FULL_SYNC_INTERVAL_SECONDS", default="1800")))
    run.add_argument(
        "--periodic-full-with-api",
        action="store_true",
        default=env_bool("REGISTRY_PERIODIC_FULL_WITH_API", False),
        help="Include unbounded external API pagination in the serial periodic full sync.",
    )
    run.add_argument("--outbox-publish-seconds", type=float, default=float(env_first("REGISTRY_OUTBOX_PUBLISH_INTERVAL_SECONDS", default="10")))
    run.add_argument("--api-delta-seconds", type=float, default=float(env_first("REGISTRY_API_DELTA_INTERVAL_SECONDS", default="30")))
    run.add_argument("--api-delta-limit", type=int, default=int(env_first("REGISTRY_API_DELTA_LIMIT", default="500")))
    run.add_argument("--clob-markets-seconds", type=float, default=float(env_first("REGISTRY_CLOB_MARKETS_INTERVAL_SECONDS", default="120")))
    run.add_argument("--clob-markets-page-budget", type=int, default=int(env_first("REGISTRY_CLOB_MARKETS_PAGE_BUDGET", default="10")))
    run.add_argument("--clob-markets-rescan-seconds", type=float, default=float(env_first("REGISTRY_CLOB_MARKETS_RESCAN_SECONDS", default="1800")))
    run.add_argument(
        "--stale-recheck-seconds",
        type=float,
        default=float(env_first("REGISTRY_STALE_RECHECK_INTERVAL_SECONDS", default="300")),
    )
    run.add_argument(
        "--stale-recheck-limit",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_LIMIT", default="100")),
    )
    run.add_argument(
        "--stale-recheck-source-age-seconds",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_SOURCE_AGE_SECONDS", default="604800")),
    )
    run.add_argument(
        "--stale-recheck-retry-seconds",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_RETRY_SECONDS", default="21600")),
    )
    run.add_argument(
        "--stale-recheck-concurrency",
        type=int,
        default=int(env_first("REGISTRY_STALE_RECHECK_CONCURRENCY", default="4")),
    )
    run.add_argument("--lob-projection-seconds", type=float, default=float(env_first("REGISTRY_LOB_PROJECTION_INTERVAL_SECONDS", default="300")))
    run.add_argument("--skip-startup-probe", action="store_true", help="Start polling/subscriptions before the first CLOB book probe.")
    run.add_argument("--skip-startup-lob-projection", action="store_true", help="Use the current LOB projection at startup and refresh it on its periodic schedule.")
    run.add_argument("--skip-startup-republish", action="store_true", help="Keep the persisted desired/outbox state instead of republishing every token at restart.")
    run.add_argument("--run-seconds", type=float, default=0.0)
    run.add_argument("--once", action="store_true", help="Run startup full sync, startup probe, republish subscriptions, then exit.")
    run.add_argument("--allow-start-without-full-sync", action="store_true")
    run.add_argument("--skip-startup-full-sync", action="store_true", help="Start from the DB/LOB projection and defer heavy full sync to the periodic loop.")
    run.add_argument(
        "--no-embedded-delta-poll",
        action="store_true",
        help="Run DB delta polling in the serial main loop instead of its independent worker.",
    )
    run.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=float(env_first("REGISTRY_HEARTBEAT_INTERVAL_SECONDS", default="30")),
    )
    run.add_argument("--no-embedded-heartbeat", action="store_true")
    run.add_argument("--no-ws", action="store_true", help="Disable the embedded market lifecycle WebSocket worker.")
    run.add_argument("--ws-url", default=DEFAULT_WS_URL)
    run.add_argument("--ws-subscription-limit", type=int, default=0, help="Max desired asset IDs for WS; 0 means all.")
    run.add_argument("--ws-refresh-seconds", type=float, default=float(env_first("REGISTRY_WS_REFRESH_INTERVAL_SECONDS", default="60")))
    run.add_argument("--ws-reconnect-seconds", type=float, default=5.0)
    run.add_argument("--ws-reconnect-max-seconds", type=float, default=60.0)
    run.add_argument(
        "--ws-health-grace-seconds",
        type=float,
        default=float(env_first("REGISTRY_WS_HEALTH_GRACE_SECONDS", default="300")),
        help="Treat WS as healthy for this many seconds after the last successful connect/message.",
    )
    run.add_argument(
        "--ws-mark-assets-disconnected-on-drop",
        action="store_true",
        help="Legacy mode: mark watched assets disconnected when the lifecycle WS drops.",
    )
    run.add_argument("--ws-ping-interval", type=float, default=float(env_first("REGISTRY_WS_PING_INTERVAL_SECONDS", default="20")))
    run.add_argument(
        "--ws-application-ping-interval",
        type=float,
        default=float(env_first("REGISTRY_WS_APPLICATION_PING_INTERVAL_SECONDS", default="10")),
        help="Polymarket text PING interval in seconds; 0 disables application heartbeats.",
    )
    run.add_argument(
        "--ws-ping-timeout",
        type=float,
        default=float(env_first("REGISTRY_WS_PING_TIMEOUT_SECONDS", default="0")),
        help="WebSocket ping timeout in seconds; 0 disables ping timeout while keeping pings enabled.",
    )
    run.add_argument("--ws-batch-size", type=int, default=500)
    ws = sub.add_parser("ws-lifecycle", parents=[common])
    ws.add_argument("--ws-url", default=DEFAULT_WS_URL)
    ws.add_argument("--max-messages", type=int, default=0)
    ws.add_argument("--run-seconds", type=float, default=0.0)
    ws.add_argument("--reconnect-seconds", type=float, default=5.0)
    ws.add_argument("--ws-reconnect-max-seconds", type=float, default=60.0)
    ws.add_argument(
        "--ws-health-grace-seconds",
        type=float,
        default=float(env_first("REGISTRY_WS_HEALTH_GRACE_SECONDS", default="300")),
    )
    ws.add_argument("--ws-mark-assets-disconnected-on-drop", action="store_true")
    ws.add_argument("--ws-subscription-limit", type=int, default=0)
    ws.add_argument("--ws-refresh-seconds", type=float, default=60.0)
    ws.add_argument("--ws-ping-interval", type=float, default=float(env_first("REGISTRY_WS_PING_INTERVAL_SECONDS", default="20")))
    ws.add_argument(
        "--ws-application-ping-interval",
        type=float,
        default=float(env_first("REGISTRY_WS_APPLICATION_PING_INTERVAL_SECONDS", default="10")),
        help="Polymarket text PING interval in seconds; 0 disables application heartbeats.",
    )
    ws.add_argument(
        "--ws-ping-timeout",
        type=float,
        default=float(env_first("REGISTRY_WS_PING_TIMEOUT_SECONDS", default="0")),
        help="WebSocket ping timeout in seconds; 0 disables ping timeout while keeping pings enabled.",
    )
    ws.add_argument("--ws-batch-size", type=int, default=500)
    sub.add_parser("registry-status", parents=[common])
    sub.add_parser("health", parents=[common])
    sub.add_parser("print-outbox", parents=[common])
    return parser


def _api_config_from_args(args: argparse.Namespace) -> MarketRegistryApiConfig:
    return MarketRegistryApiConfig(
        gamma_api_base=args.gamma_api_base,
        clob_api_base=args.clob_api_base,
        timeout_seconds=float(args.api_timeout_seconds),
        proxy_mode=args.proxy_mode,
        proxy_url=args.proxy_url,
        fallback_proxy_urls=args.fallback_proxy_urls,
        trust_env_proxy=bool(args.trust_env_proxy),
    )


def _init_schema_if_needed(service: MarketRegistryService, args: argparse.Namespace) -> None:
    if not bool(getattr(args, "skip_init_schema", False)):
        service.init_schema()


@contextlib.contextmanager
def _api_context(args: argparse.Namespace):
    if args.no_api:
        yield None
        return
    yield PolymarketApiClient(_api_config_from_args(args))


class _EmbeddedWsLifecycleWorker:
    def __init__(self, args: argparse.Namespace, config: MarketUniverseConfig) -> None:
        self.args = args
        self.config = config
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_error: str | None = None
        self.last_heartbeat_monotonic: float | None = None
        self.ws_health_grace_seconds = max(0.0, float(getattr(args, "ws_health_grace_seconds", 300.0)))
        self.lock = threading.Lock()

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="paper-market-registry-ws", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.connected_event.clear()
        if self.thread is not None:
            self.thread.join(timeout=5.0)

    def is_connected(self) -> bool:
        if self.connected_event.is_set():
            return True
        with self.lock:
            heartbeat = self.last_heartbeat_monotonic
        if heartbeat is None:
            return False
        return time.monotonic() - heartbeat <= self.ws_health_grace_seconds

    def _set_connected(self, value: bool) -> None:
        if value:
            self.connected_event.set()
            with self.lock:
                self.last_heartbeat_monotonic = time.monotonic()
        else:
            self.connected_event.clear()

    def _run(self) -> None:
        try:
            asyncio.run(_run_embedded_ws_lifecycle_loop(self.args, self.config, self.stop_event, self._set_connected))
        except Exception as exc:  # noqa: BLE001 - background lifecycle should not kill polling/probe loops.
            self.last_error = str(exc)
            self._set_connected(False)
            print(_json({"status": "ws_worker_stopped", "error": str(exc)}), flush=True)


@contextlib.contextmanager
def _embedded_ws_lifecycle_worker(args: argparse.Namespace, config: MarketUniverseConfig):
    if getattr(args, "once", False) or getattr(args, "no_ws", False):
        yield None
        return
    worker = _EmbeddedWsLifecycleWorker(args, config)
    worker.start()
    try:
        yield worker
    finally:
        worker.stop()


class _EmbeddedRegistryHeartbeatWorker:
    """Persist registry liveness independently from discovery and probe writes."""

    def __init__(self, args: argparse.Namespace, ws_worker: _EmbeddedWsLifecycleWorker | None) -> None:
        self.args = args
        self.ws_worker = ws_worker
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_error: str | None = None
        self.completed_cycles = 0

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="paper-market-registry-heartbeat", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5.0)

    def _run(self) -> None:
        interval = max(5.0, float(self.args.heartbeat_seconds))
        started = time.monotonic()
        try:
            with postgres_connection(readonly=False) as conn:
                repo = MarketRegistryRepository(conn)
                while not self.stop_event.is_set():
                    try:
                        repo.record_health_snapshot(
                            label="heartbeat",
                            uptime_seconds=time.monotonic() - started,
                            ws_connected=self.ws_worker.is_connected() if self.ws_worker is not None else False,
                            meta={"source": "embedded_heartbeat"},
                        )
                        conn.commit()
                        self.completed_cycles += 1
                        self.last_error = None
                    except Exception as exc:  # noqa: BLE001 - heartbeat retries on the next interval.
                        conn.rollback()
                        self.last_error = str(exc)
                        print(_json({"status": "heartbeat_worker_error", "error": str(exc)}), flush=True)
                    self.stop_event.wait(interval)
        except Exception as exc:  # noqa: BLE001 - discovery remains available if heartbeat setup fails.
            self.last_error = str(exc)
            print(_json({"status": "heartbeat_worker_stopped", "error": str(exc)}), flush=True)


@contextlib.contextmanager
def _embedded_registry_heartbeat_worker(
    args: argparse.Namespace,
    ws_worker: _EmbeddedWsLifecycleWorker | None,
):
    disabled = (
        getattr(args, "once", False)
        or getattr(args, "no_embedded_heartbeat", False)
        or float(getattr(args, "heartbeat_seconds", 0.0)) <= 0
    )
    if disabled:
        yield None
        return
    worker = _EmbeddedRegistryHeartbeatWorker(args, ws_worker)
    worker.start()
    try:
        yield worker
    finally:
        worker.stop()


class _EmbeddedDeltaPollWorker:
    """Keep the DB-backed delta path alive while API/probe work is blocked."""

    def __init__(self, args: argparse.Namespace, config: MarketUniverseConfig) -> None:
        self.args = args
        self.config = config
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_error: str | None = None
        self.completed_cycles = 0

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="paper-market-registry-delta", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=10.0)

    def _run(self) -> None:
        interval = max(1.0, float(self.args.poll_seconds))
        try:
            with postgres_connection(readonly=False) as conn:
                service = MarketRegistryService(conn, config=self.config, api_client=None)
                while not self.stop_event.is_set():
                    started = time.monotonic()
                    try:
                        service.delta_poll(
                            limit=self.args.limit,
                            include_api=False,
                            api_limit=0,
                            api_page_size=self.args.api_page_size,
                        )
                        conn.commit()
                        self.completed_cycles += 1
                        self.last_error = None
                    except Exception as exc:  # noqa: BLE001 - the next interval must retry.
                        conn.rollback()
                        self.last_error = str(exc)
                        print(_json({"status": "delta_worker_error", "error": str(exc)}), flush=True)
                    wait_seconds = max(0.1, interval - (time.monotonic() - started))
                    self.stop_event.wait(wait_seconds)
        except Exception as exc:  # noqa: BLE001 - main API/probe loop remains available.
            self.last_error = str(exc)
            print(_json({"status": "delta_worker_stopped", "error": str(exc)}), flush=True)


@contextlib.contextmanager
def _embedded_delta_poll_worker(args: argparse.Namespace, config: MarketUniverseConfig):
    disabled = (
        getattr(args, "once", False)
        or getattr(args, "no_embedded_delta_poll", False)
        or float(getattr(args, "poll_seconds", 0.0)) <= 0
    )
    if disabled:
        yield None
        return
    worker = _EmbeddedDeltaPollWorker(args, config)
    worker.start()
    try:
        yield worker
    finally:
        worker.stop()


async def _run_embedded_ws_lifecycle_loop(
    args: argparse.Namespace,
    config: MarketUniverseConfig,
    stop_event: threading.Event,
    connection_state_callback: Callable[[bool], None],
) -> None:
    with _api_context(args) as api_client:
        with postgres_connection(readonly=False) as conn:
            service = MarketRegistryService(conn, config=config, api_client=api_client)
            repo = MarketRegistryRepository(conn)
            _init_schema_if_needed(service, args)
            conn.commit()
            await run_ws_lifecycle_loop(
                service,
                repo,
                args,
                stop_event=stop_event,
                connection_state_callback=connection_state_callback,
            )


async def run_ws_lifecycle_loop(
    service: MarketRegistryService,
    repo: MarketRegistryRepository,
    args: argparse.Namespace,
    *,
    stop_event: threading.Event | None = None,
    connection_state_callback: Callable[[bool], None] | None = None,
) -> None:
    import websockets

    started = time.monotonic()
    messages = 0
    watched_asset_ids: set[str] = set()
    reconnect_attempts = 0
    max_messages = int(getattr(args, "max_messages", 0) or 0)
    reconnect_seconds = max(0.1, float(getattr(args, "reconnect_seconds", getattr(args, "ws_reconnect_seconds", 5.0))))
    reconnect_max_seconds = max(reconnect_seconds, float(getattr(args, "ws_reconnect_max_seconds", 60.0)))
    refresh_seconds = max(1.0, float(getattr(args, "ws_refresh_seconds", 60.0)))
    application_ping_seconds = _positive_float_or_none(
        getattr(args, "ws_application_ping_interval", 10.0)
    )
    ws_url = str(getattr(args, "ws_url", DEFAULT_WS_URL))
    ws_proxy_candidates = _ws_proxy_candidates(args)
    ws_proxy_index = 0
    while not _stop_requested(stop_event):
        if args.run_seconds and time.monotonic() - started >= float(args.run_seconds):
            return
        try:
            ws_proxy = ws_proxy_candidates[ws_proxy_index]
            async with websockets.connect(
                ws_url,
                ping_interval=_positive_float_or_none(getattr(args, "ws_ping_interval", 20.0)),
                ping_timeout=_positive_float_or_none(getattr(args, "ws_ping_timeout", 0.0)),
                open_timeout=20,
                proxy=ws_proxy,
            ) as websocket:
                reconnect_attempts = 0
                _set_ws_connected(connection_state_callback, True)
                # A subscription is scoped to one TCP/WebSocket connection.
                # Reusing the previous connection's in-memory set makes the
                # reconnect send no initial payload; its first text heartbeat
                # is then rejected by CLOB as an invalid subscription. Replay
                # the bounded desired set on every new connection.
                watched_asset_ids = await _refresh_ws_subscriptions(
                    websocket,
                    repo,
                    set(),
                    args,
                    initial=True,
                )
                last_refresh_at = time.monotonic()
                last_application_ping_at = time.monotonic()
                _record_ws_health(
                    repo,
                    label="ws_connected",
                    connected=True,
                    meta={
                        "ws_url": ws_url,
                        "watched_asset_count": len(watched_asset_ids),
                        "proxy_route": _ws_proxy_label(ws_proxy),
                    },
                )
                while not _stop_requested(stop_event):
                    if args.run_seconds and time.monotonic() - started >= float(args.run_seconds):
                        return
                    now = time.monotonic()
                    if application_ping_seconds is not None and (
                        now - last_application_ping_at >= application_ping_seconds
                    ):
                        await websocket.send("PING")
                        last_application_ping_at = now
                    if now - last_refresh_at >= refresh_seconds:
                        watched_asset_ids = await _refresh_ws_subscriptions(
                            websocket,
                            repo,
                            watched_asset_ids,
                            args,
                            initial=False,
                        )
                        last_refresh_at = now
                        _set_ws_connected(connection_state_callback, True)
                        _record_ws_health(
                            repo,
                            label="ws_connected",
                            connected=True,
                            meta={
                                "ws_url": ws_url,
                                "watched_asset_count": len(watched_asset_ids),
                                "proxy_route": _ws_proxy_label(ws_proxy),
                                "refresh": True,
                            },
                        )
                    deadlines = [last_refresh_at + refresh_seconds]
                    if application_ping_seconds is not None:
                        deadlines.append(last_application_ping_at + application_ping_seconds)
                    receive_timeout = max(0.1, min(deadlines) - time.monotonic())
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=receive_timeout)
                    except asyncio.TimeoutError:
                        continue
                    _set_ws_connected(connection_state_callback, True)
                    for event in _decode_ws_payload(raw):
                        event_type = str(event.get("event_type") or event.get("type") or "")
                        if event_type in {"new_market", "market_resolved"}:
                            _process_ws_lifecycle_event(service, repo, event, event_type=event_type)
                        messages += 1
                        if max_messages and messages >= max_messages:
                            return
        except KeyboardInterrupt:
            return
        except Exception as exc:  # noqa: BLE001
            reconnect_attempts += 1
            failed_proxy = ws_proxy_candidates[ws_proxy_index]
            ws_proxy_index = (ws_proxy_index + 1) % len(ws_proxy_candidates)
            _set_ws_connected(connection_state_callback, False)
            with contextlib.suppress(Exception):
                service.conn.rollback()
            mark_assets = bool(getattr(args, "ws_mark_assets_disconnected_on_drop", False))
            if watched_asset_ids and mark_assets:
                disconnect_result = service.handle_ws_disconnect(sorted(watched_asset_ids), reason=str(exc))
                service.conn.commit()
                print(
                    _json(
                        {
                            "event_type": "WS_DISCONNECTED",
                            "run_id": disconnect_result.get("run_id"),
                            "affected_count": len(disconnect_result.get("affected_asset_ids") or []),
                            "reason": str(exc),
                        }
                    ),
                    flush=True,
                )
            elif watched_asset_ids:
                print(
                    _json(
                        {
                            "event_type": "WS_RECONNECTING",
                            "affected_count": 0,
                            "watched_asset_count": len(watched_asset_ids),
                            "reason": str(exc),
                            "asset_state_unchanged": True,
                        }
                    ),
                    flush=True,
                )
            _record_ws_health(
                repo,
                label="ws_reconnecting",
                connected=False,
                meta={
                    "ws_url": ws_url,
                    "watched_asset_count": len(watched_asset_ids),
                    "reason": str(exc),
                    "attempt": reconnect_attempts,
                    "failed_proxy_route": _ws_proxy_label(failed_proxy),
                    "next_proxy_route": _ws_proxy_label(ws_proxy_candidates[ws_proxy_index]),
                    "asset_state_unchanged": not mark_assets,
                },
            )
            delay = _reconnect_delay(
                base_seconds=reconnect_seconds,
                max_seconds=reconnect_max_seconds,
                attempt=reconnect_attempts,
            )
            print(_json({"status": "reconnecting", "error": str(exc), "delay_seconds": round(delay, 3)}))
            await _sleep_until_reconnect_or_stop(delay, stop_event)
        finally:
            _set_ws_connected(connection_state_callback, False)


async def _refresh_ws_subscriptions(
    websocket: Any,
    repo: MarketRegistryRepository,
    current_asset_ids: set[str],
    args: argparse.Namespace,
    *,
    initial: bool = False,
) -> set[str]:
    limit = _limit_arg(getattr(args, "ws_subscription_limit", 0))
    desired_asset_ids = set(repo.list_desired_subscription_asset_ids(limit=limit))
    added = desired_asset_ids - current_asset_ids
    removed = current_asset_ids - desired_asset_ids
    batch_size = max(1, int(getattr(args, "ws_batch_size", 500) or 500))
    await _send_ws_subscription_batches(
        websocket,
        added,
        batch_size=batch_size,
        subscribe=True,
        initial=initial,
    )
    await _send_ws_subscription_batches(websocket, removed, batch_size=batch_size, subscribe=False)
    return desired_asset_ids


async def _send_ws_subscription_batches(
    websocket: Any,
    asset_ids: set[str],
    *,
    batch_size: int,
    subscribe: bool,
    initial: bool = False,
) -> None:
    for batch_index, batch in enumerate(_chunks(sorted(asset_ids), batch_size)):
        payload: dict[str, Any] = {"type": "market", "assets_ids": batch}
        if subscribe:
            payload["custom_feature_enabled"] = True
            # Only the first payload on a fresh connection has the initial
            # schema. Every additional chunk and every later refresh is an
            # explicit subscribe operation.
            if not initial or batch_index > 0:
                payload["operation"] = "subscribe"
        else:
            payload["operation"] = "unsubscribe"
        await websocket.send(json.dumps(payload))


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _set_ws_connected(callback: Callable[[bool], None] | None, value: bool) -> None:
    if callback is not None:
        callback(value)


def _record_ws_health(repo: MarketRegistryRepository, *, label: str, connected: bool, meta: dict[str, Any]) -> None:
    try:
        repo.record_health_snapshot(label=label, ws_connected=connected, meta=meta)
        repo.conn.commit()
    except Exception:  # noqa: BLE001 - WS health should not kill the lifecycle feed.
        with contextlib.suppress(Exception):
            repo.conn.rollback()


def _process_ws_lifecycle_event(
    service: MarketRegistryService,
    repo: MarketRegistryRepository,
    event: dict[str, Any],
    *,
    event_type: str,
) -> dict[str, Any]:
    """Keep a single bad lifecycle DB transaction from dropping the WS feed."""

    try:
        result = service.handle_lifecycle_event(event)
        service.conn.commit()
        print(_json(result), flush=True)
        return result
    except Exception as exc:  # noqa: BLE001 - the feed must survive one bad event.
        with contextlib.suppress(Exception):
            service.conn.rollback()
        result = {
            "event_type": "LIFECYCLE_EVENT_PROCESSING_FAILED",
            "lifecycle_event_type": event_type,
            "reason": str(exc),
            "ws_connection_preserved": True,
        }
        print(_json(result), flush=True)
        _record_ws_health(
            repo,
            label="ws_lifecycle_event_failed",
            connected=True,
            meta=result,
        )
        return result


def _ws_proxy_from_args(args: argparse.Namespace) -> str | Literal[True] | None:
    return _ws_proxy_candidates(args)[0]


def _ws_proxy_candidates(args: argparse.Namespace) -> list[str | Literal[True] | None]:
    mode = str(getattr(args, "proxy_mode", "direct") or "direct").strip().lower()
    proxy_url = str(getattr(args, "proxy_url", "") or "").strip()
    fallback_urls = [
        value.strip()
        for value in str(getattr(args, "fallback_proxy_urls", "") or "").split(",")
        if value.strip()
    ]
    if mode == "explicit":
        candidates = list(dict.fromkeys([value for value in [proxy_url, *fallback_urls] if value]))
        return candidates or [None]
    if mode == "env" or bool(getattr(args, "trust_env_proxy", False)):
        return [True]
    return [None]


def _ws_proxy_label(proxy: str | Literal[True] | None) -> str:
    if proxy is True:
        return "environment"
    return str(proxy or "direct")


def _positive_float_or_none(value: float | int | str | None) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if number > 0 else None


def _reconnect_delay(*, base_seconds: float, max_seconds: float, attempt: int) -> float:
    exponent = min(6, max(0, int(attempt) - 1))
    delay = min(float(max_seconds), float(base_seconds) * (2**exponent))
    jitter = random.uniform(0.0, min(1.0, delay * 0.2))
    return max(0.1, delay + jitter)


def _stop_requested(stop_event: threading.Event | None) -> bool:
    return bool(stop_event is not None and stop_event.is_set())


async def _sleep_until_reconnect_or_stop(seconds: float, stop_event: threading.Event | None) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if _stop_requested(stop_event):
            return
        await asyncio.sleep(min(0.5, max(0.0, deadline - time.monotonic())))


def _decode_ws_payload(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        text = raw.strip()
        if not text or text.upper() in {"PING", "PONG"}:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
    else:
        parsed = raw
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def _read_outbox(repo: MarketRegistryRepository, *, limit: int) -> list[dict[str, Any]]:
    limit_sql = ""
    params: tuple[Any, ...] = ()
    if limit > 0:
        limit_sql = "LIMIT %s"
        params = (int(limit),)
    with repo.conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT outbox_id, event_type, asset_id, market_id, condition_id, status, payload, created_at
            FROM quant.paper_registry_outbox
            ORDER BY outbox_id DESC
            {limit_sql}
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _limit_arg(value: int | None) -> int | None:
    if value is None:
        return None
    return None if int(value) <= 0 else int(value)


def _bounded_cycle_limit(requested: int, cycle_limit: int) -> int:
    if int(cycle_limit) <= 0:
        return int(requested)
    if int(requested) <= 0:
        return int(cycle_limit)
    return min(int(requested), int(cycle_limit))


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
