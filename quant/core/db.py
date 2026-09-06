"""Database and ClickHouse connection helpers for quant price builders."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

psycopg: Any
dict_row: Any
try:
    import psycopg as psycopg
    from psycopg.rows import dict_row as dict_row
except ImportError:  # pragma: no cover - exercised only in under-provisioned envs
    psycopg = None
    dict_row = None


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv_files() -> None:
    if str(os.environ.get("POLY_QUANT_DISABLE_DOTENV", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    explicit = os.environ.get("BACKTEST_LAB_ENV_FILE")
    if explicit:
        candidate = Path(explicit).expanduser()
        if not candidate.is_file():
            raise FileNotFoundError("BACKTEST_LAB_ENV_FILE must name an existing configuration file")
        load_dotenv(candidate, override=False)
    for candidate in (
        PROJECT_ROOT / ".env",
        PROJECT_ROOT / ".env.local",
        PROJECT_ROOT / "scripts" / ".env",
    ):
        if candidate.exists():
            load_dotenv(candidate, override=False)


load_dotenv_files()


def env_first(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip()
    return default


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return default


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def env_int_first(*names: str, default: int) -> int:
    for name in names:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return default
    return default


def env_float_first(*names: str, default: float) -> float:
    for name in names:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            return float(str(raw).strip())
        except (TypeError, ValueError):
            return default
    return default


@dataclass(frozen=True)
class PostgresSettings:
    host: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_POSTGRES_HOST",
            "POLYMARKET_POSTGRES_HOST",
            "POLYMARKET_PostgreSQL_HOST",
            default="127.0.0.1",
        )
    )
    port: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_POSTGRES_PORT",
            "POLYMARKET_POSTGRES_PORT",
            "POLYMARKET_PostgreSQL_PORT",
            default=45432,
        )
    )
    user: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_POSTGRES_USER",
            "POLYMARKET_POSTGRES_USER",
            "POLYMARKET_PostgreSQL_USER",
            default="poly_user",
        )
    )
    password: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_POSTGRES_PASSWORD",
            "POLYMARKET_POSTGRES_PASSWORD",
            "POLYMARKET_POSTGRESQL_PASSWORD",
            "POLYMARKET_PostgreSQL_PASSWORD",
            default="",
        )
    )
    database: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_POSTGRES_DATABASE",
            "POLYMARKET_POSTGRES_DATABASE",
            "POLYMARKET_PostgreSQL_DATABASE",
            default="poly_data_core",
        )
    )
    search_path: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_POSTGRES_SEARCH_PATH",
            default="quant,core,oracle,ops,public",
        )
    )
    connect_timeout_seconds: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_CONNECT_TIMEOUT_SECONDS",
            default=10,
        )
    )
    statement_timeout_ms: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_STATEMENT_TIMEOUT_MS",
            default=0,
        )
    )
    lock_timeout_ms: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_LOCK_TIMEOUT_MS",
            default=0,
        )
    )
    tcp_user_timeout_ms: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_TCP_USER_TIMEOUT_MS",
            default=0,
        )
    )
    keepalives_idle_seconds: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_KEEPALIVES_IDLE_SECONDS",
            default=0,
        )
    )
    keepalives_interval_seconds: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_KEEPALIVES_INTERVAL_SECONDS",
            default=0,
        )
    )
    keepalives_count: int = field(
        default_factory=lambda: env_int_first(
            "POLYDATA_QUANT_POSTGRES_KEEPALIVES_COUNT",
            default=0,
        )
    )


@dataclass(frozen=True)
class ClickHouseSettings:
    http_url: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_HTTP_URL", default=""
        )
    )
    container: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_CONTAINER",
            default="polydata_clickhouse_orderfilled",
        )
    )
    database: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_DATABASE", default="poly_orderfilled"
        )
    )
    user: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_USER", default="poly_user"
        )
    )
    password: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_PASSWORD",
            "CLICKHOUSE_PASSWORD",
            default="",
        )
    )
    orderfilled_table: str = field(
        default_factory=lambda: env_first(
            "POLYDATA_ORDERFILLED_CLICKHOUSE_READ_TABLE", default="orderfilled_fact"
        )
    )
    timeout_seconds: float = field(
        default_factory=lambda: env_float_first(
            "POLYDATA_QUANT_CLICKHOUSE_TIMEOUT_SECONDS", default=120.0
        )
    )


def database_settings_summary(
    postgres: PostgresSettings | None = None,
    clickhouse: ClickHouseSettings | None = None,
) -> dict[str, Any]:
    pg = postgres or PostgresSettings()
    ch = clickhouse or ClickHouseSettings()
    return {
        "postgres": {
            "host": pg.host,
            "port": pg.port,
            "user": pg.user,
            "database": pg.database,
            "search_path": pg.search_path,
            "connect_timeout_seconds": pg.connect_timeout_seconds,
            "statement_timeout_ms": pg.statement_timeout_ms,
            "lock_timeout_ms": pg.lock_timeout_ms,
            "tcp_user_timeout_ms": pg.tcp_user_timeout_ms,
            "password_configured": bool(pg.password),
        },
        "clickhouse": {
            "http_url_configured": bool(ch.http_url),
            "container": ch.container,
            "database": ch.database,
            "user": ch.user,
            "password_configured": bool(ch.password),
            "orderfilled_table": ch.orderfilled_table,
        },
    }


def safe_identifier(value: str, *, default: str | None = None) -> str:
    text = str(value or default or "").strip()
    if not text:
        raise ValueError("identifier is required")
    if not all(ch.isalnum() or ch == "_" for ch in text):
        raise ValueError(f"unsafe identifier: {value!r}")
    return text


def _open_postgres_connection(settings: PostgresSettings) -> Any:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Install psycopg[binary] first.")
    connection_params: dict[str, Any] = {
        "host": settings.host,
        "port": settings.port,
        "user": settings.user,
        "password": settings.password,
        "dbname": settings.database,
        "connect_timeout": settings.connect_timeout_seconds,
        "row_factory": dict_row,
        "autocommit": False,
    }
    startup_options: list[str] = []
    if settings.statement_timeout_ms > 0:
        startup_options.append(f"-c statement_timeout={settings.statement_timeout_ms}")
    if settings.lock_timeout_ms > 0:
        startup_options.append(f"-c lock_timeout={settings.lock_timeout_ms}")
    if startup_options:
        connection_params["options"] = " ".join(startup_options)
    for key, value in (
        ("tcp_user_timeout", settings.tcp_user_timeout_ms),
        ("keepalives_idle", settings.keepalives_idle_seconds),
        ("keepalives_interval", settings.keepalives_interval_seconds),
        ("keepalives_count", settings.keepalives_count),
    ):
        if value > 0:
            connection_params[key] = value
    conn = psycopg.connect(
        **connection_params,
    )
    if settings.search_path:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO " + settings.search_path)
    with conn.cursor() as cur:
        cur.execute("SET max_parallel_workers_per_gather = 0")
        cur.execute("SET work_mem = '32MB'")
    conn.commit()
    return conn


@contextmanager
def postgres_connection(
    settings: PostgresSettings | None = None,
    *,
    readonly: bool = False,
) -> Iterator[Any]:
    cfg = settings or PostgresSettings()
    conn = _open_postgres_connection(cfg)
    try:
        if readonly:
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
        yield conn
        if not readonly:
            conn.commit()
    except Exception:
        if not readonly:
            conn.rollback()
        raise
    finally:
        conn.close()


class ThreadLocalPostgresConnectionFactory:
    """Reuse one transaction-safe connection per worker thread."""

    def __init__(self, settings: PostgresSettings | None = None) -> None:
        self.settings = settings or PostgresSettings()
        self._local = threading.local()

    def _connection(self) -> Any:
        conn = getattr(self._local, "connection", None)
        if conn is None or bool(conn.closed) or bool(conn.broken):
            conn = _open_postgres_connection(self.settings)
            self._local.connection = conn
        return conn

    def _discard(self, conn: Any) -> None:
        try:
            conn.close()
        finally:
            if getattr(self._local, "connection", None) is conn:
                self._local.connection = None

    @contextmanager
    def __call__(self, *, readonly: bool = False) -> Iterator[Any]:
        conn = self._connection()
        try:
            if readonly:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
            yield conn
            if readonly:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                self._discard(conn)
            if bool(getattr(conn, "broken", False)):
                self._discard(conn)
            raise


class ClickHouseClient:
    """Small ClickHouse reader using HTTP tunnel when available, else docker exec."""

    def __init__(self, settings: ClickHouseSettings | None = None) -> None:
        self.settings = settings or ClickHouseSettings()
        safe_identifier(self.settings.database)
        safe_identifier(self.settings.orderfilled_table)

    def query_json_rows(
        self, query: str, *, timeout_seconds: float | None = None
    ) -> list[dict[str, Any]]:
        full_query = query.rstrip().removesuffix(";") + "\nFORMAT JSONEachRow"
        output = self._query_text(full_query, timeout_seconds=timeout_seconds)
        rows: list[dict[str, Any]] = []
        for line in output.splitlines():
            text = line.strip()
            if not text:
                continue
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                rows.append(parsed)
        return rows

    def query_scalar(self, query: str, *, timeout_seconds: float | None = None) -> str:
        return self._query_text(
            query.rstrip().removesuffix(";"), timeout_seconds=timeout_seconds
        ).strip()

    def execute(
        self,
        query: str,
        *,
        stdin: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._query_text(query, stdin=stdin, timeout_seconds=timeout_seconds)

    def _query_text(
        self,
        query: str,
        *,
        stdin: str | None = None,
        timeout_seconds: float | None = None,
    ) -> str:
        timeout = (
            self.settings.timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        if self.settings.http_url:
            return self._query_http(query, timeout_seconds=timeout, stdin=stdin)
        if shutil.which("docker") is None:
            raise RuntimeError(
                "docker is unavailable and POLYDATA_ORDERFILLED_CLICKHOUSE_HTTP_URL is not configured"
            )
        payload = query.rstrip() + "\n"
        if stdin is not None:
            payload += stdin
        child_env = os.environ.copy()
        # docker accepts an environment-variable name without its value and
        # forwards the value from the client environment.  This keeps the
        # credential out of both the host docker argv and the container's
        # clickhouse-client argv.  clickhouse-client reads CLICKHOUSE_PASSWORD
        # directly from its environment.
        child_env["CLICKHOUSE_PASSWORD"] = self.settings.password
        try:
            completed = subprocess.run(
                self._docker_cmd(),
                input=payload,
                check=True,
                text=True,
                capture_output=True,
                timeout=timeout,
                env=child_env,
            )
        except subprocess.CalledProcessError as exc:
            detail = str(
                exc.stderr or exc.stdout or "ClickHouse client returned no diagnostic"
            ).strip()
            if self.settings.password:
                detail = detail.replace(self.settings.password, "[REDACTED]")
            raise RuntimeError(
                f"ClickHouse query failed with exit code {exc.returncode}: {detail[-2000:]}"
            ) from None
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"ClickHouse query timed out after {timeout} seconds"
            ) from None
        return completed.stdout

    def _docker_cmd(self) -> list[str]:
        return [
            "docker",
            "exec",
            "-i",
            "--env",
            "CLICKHOUSE_PASSWORD",
            self.settings.container,
            "clickhouse-client",
            "--user",
            self.settings.user,
            "--database",
            self.settings.database,
        ]

    def _query_http(
        self, query: str, *, timeout_seconds: float, stdin: str | None = None
    ) -> str:
        params = urlencode(
            {
                "database": self.settings.database,
                "user": self.settings.user,
                "password": self.settings.password,
            }
        )
        separator = "&" if "?" in self.settings.http_url else "?"
        body = query if stdin is None else query.rstrip() + "\n" + stdin
        request = Request(
            f"{self.settings.http_url}{separator}{params}",
            data=body.encode("utf-8"),
            method="POST",
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read().decode("utf-8", errors="replace")


def execute_many(conn: Any, sql: str, rows: Sequence[Sequence[Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount if cur.rowcount is not None else len(rows)
