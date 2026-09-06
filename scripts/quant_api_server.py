#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quant-only API server for prediction-market-quant."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

_scripts_root = Path(__file__).resolve().parent
_repo_root = _scripts_root.parent
for candidate in (_repo_root, _scripts_root):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

try:
    from flask import Flask, Response, g, jsonify, request
except ImportError:
    print("Error: flask not installed. pip install flask", file=sys.stderr)
    sys.exit(1)

try:
    from werkzeug.exceptions import HTTPException
except ImportError:  # pragma: no cover
    HTTPException = Exception

from api.routes.quant import create_quant_blueprint
from quant.core.db import PostgresSettings, database_settings_summary, postgres_connection
from quant.core.schema import create_schema


app = Flask(__name__)
_memory_cache: dict[str, tuple[float, Any]] = {}
_memory_cache_lock = threading.Lock()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _cache_key(namespace: str, cache_key: str) -> str:
    return f"{namespace}:{cache_key}"


def get_cached_json(namespace: str, cache_key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    key = _cache_key(namespace, cache_key)
    with _memory_cache_lock:
        item = _memory_cache.get(key)
        if item is None:
            return None
        expires_at, payload = item
        if expires_at <= now:
            _memory_cache.pop(key, None)
            return None
    return payload if isinstance(payload, dict) else None


def set_cached_json(namespace: str, cache_key: str, payload: Dict[str, Any], ttl_seconds: int) -> None:
    with _memory_cache_lock:
        _memory_cache[_cache_key(namespace, cache_key)] = (time.time() + max(1, int(ttl_seconds)), payload)


def get_snapshot_payload(namespace: str, cache_key: str, builder, *, ttl_seconds: int) -> Any:
    cached = get_cached_json(namespace, cache_key)
    if cached is not None:
        return cached
    payload = builder()
    if isinstance(payload, dict):
        set_cached_json(namespace, cache_key, payload, ttl_seconds)
    return payload


def register_routes() -> None:
    if app.config.get("QUANT_BLUEPRINT_REGISTERED"):
        return
    app.register_blueprint(create_quant_blueprint({
        "app": app,
        "get_cached_json": get_cached_json,
        "set_cached_json": set_cached_json,
        "get_snapshot_payload": get_snapshot_payload,
    }))
    app.config["QUANT_BLUEPRINT_REGISTERED"] = True


@app.before_request
def before_request() -> None:
    g.request_id = str(uuid.uuid4())
    g.started_at = time.perf_counter()
    app.logger.info(
        "request-start request_id=%s method=%s path=%s query=%s remote=%s",
        g.request_id,
        request.method,
        request.path,
        request.query_string.decode("utf-8", errors="replace"),
        request.remote_addr,
    )


@app.after_request
def after_request(response):
    duration_ms = (time.perf_counter() - getattr(g, "started_at", time.perf_counter())) * 1000
    app.logger.info(
        "request-end request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
        getattr(g, "request_id", "-"),
        request.method,
        request.path,
        response.status_code,
        duration_ms,
    )
    response.headers.setdefault("Access-Control-Allow-Origin", "*")
    response.headers.setdefault("Access-Control-Allow-Headers", "Content-Type, Authorization")
    response.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    return response


@app.route("/quant/health", methods=["GET"])
def quant_health():
    return Response(
        json.dumps({"status": "ok", "service": "prediction-market-quant-api", "database": database_settings_summary()}, default=_json_default),
        mimetype="application/json",
    )


@app.errorhandler(Exception)
def handle_error(error: Exception):
    if isinstance(error, HTTPException):
        return jsonify({"error": getattr(error, "description", str(error))}), int(getattr(error, "code", 500) or 500)
    app.logger.exception("Unhandled quant API error")
    return jsonify({"error": "internal server error"}), 500


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the prediction-market-quant API server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18500)
    parser.add_argument("--skip-init-schema", action="store_true")
    parser.add_argument("--log-level", default="ERROR", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.ERROR)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app.logger.setLevel(log_level)
    if not args.skip_init_schema:
        with postgres_connection(PostgresSettings(), readonly=False) as conn:
            create_schema(conn)
            conn.commit()
    register_routes()
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
