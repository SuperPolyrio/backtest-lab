"""Disk-backed cumulative ledger for bounded-memory Fill-only replay."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator, Mapping
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal

Q = Decimal("0.0000000001")
SCHEMA_VERSION = "UnifiedFillOnlyDiskLiquidityLedgerV1"


class DiskLiquidityLedger:
    """Accumulate immutable batch deltas without retaining all keys in RAM."""

    def __init__(
        self,
        path: str | Path,
        *,
        execution_family: Literal["V2", "V3"],
        ledger_id: str,
        source_pin: str,
        profile_hash: str,
        strategy_hash: str,
    ) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.execution_family = execution_family
        self.ledger_id = str(ledger_id)
        self.connection = sqlite3.connect(self.path, timeout=60)
        self.connection.create_function("decimal_add", 2, _decimal_add)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS capacities (
                namespace TEXT NOT NULL,
                capacity_key TEXT NOT NULL,
                quantity TEXT NOT NULL,
                PRIMARY KEY (namespace, capacity_key)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS applied_deltas (
                batch_number INTEGER PRIMARY KEY,
                delta_sha256 TEXT NOT NULL
            );
            """
        )
        bindings = {
            "schema_version": SCHEMA_VERSION,
            "execution_family": execution_family,
            "ledger_id": self.ledger_id,
            "source_pin": str(source_pin),
            "profile_hash": str(profile_hash),
            "strategy_hash": str(strategy_hash),
        }
        existing = dict(self.connection.execute("SELECT key, value FROM metadata"))
        if existing:
            if existing != bindings:
                raise ValueError("disk liquidity ledger binding mismatch")
        else:
            self.connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)", bindings.items()
            )
            self.connection.commit()

    def apply_delta(
        self,
        batch_number: int,
        delta_sha256: str,
        payload: Mapping[str, Any],
    ) -> bool:
        existing = self.connection.execute(
            "SELECT delta_sha256 FROM applied_deltas WHERE batch_number = ?",
            (int(batch_number),),
        ).fetchone()
        if existing is not None:
            if str(existing[0]) != str(delta_sha256):
                raise ValueError("disk liquidity ledger delta checksum mismatch")
            return False
        rows = list(_delta_rows(self.execution_family, payload))
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO capacities(namespace, capacity_key, quantity)
                VALUES (?, ?, ?)
                ON CONFLICT(namespace, capacity_key) DO UPDATE SET
                    quantity = decimal_add(capacities.quantity, excluded.quantity)
                """,
                rows,
            )
            self.connection.execute(
                "INSERT INTO applied_deltas(batch_number, delta_sha256) VALUES (?, ?)",
                (int(batch_number), str(delta_sha256)),
            )
        return True

    def has_delta(self, batch_number: int, delta_sha256: str) -> bool:
        existing = self.connection.execute(
            "SELECT delta_sha256 FROM applied_deltas WHERE batch_number = ?",
            (int(batch_number),),
        ).fetchone()
        if existing is None:
            return False
        if str(existing[0]) != str(delta_sha256):
            raise ValueError("disk liquidity ledger delta checksum mismatch")
        return True

    def canonical_sha256(self) -> str:
        digest = hashlib.sha256()
        if self.execution_family == "V2":
            self._hash_namespace_map(digest, "v2_source_capacity")
            return digest.hexdigest()
        digest.update(b'{"inferred_source_capacity":')
        self._hash_namespace_map(digest, "inferred_source_capacity")
        digest.update(b',"ledger_id":')
        digest.update(_json_string(self.ledger_id))
        digest.update(b',"synthetic_arrival_capacity":')
        self._hash_namespace_map(digest, "synthetic_arrival_capacity")
        digest.update(b',"v2_source_capacity":')
        self._hash_namespace_map(digest, "v2_source_capacity")
        digest.update(b"}")
        return digest.hexdigest()

    def as_dict(self) -> dict[str, Any]:
        if self.execution_family == "V2":
            return self._namespace_dict("v2_source_capacity")
        return {
            "ledger_id": self.ledger_id,
            "v2_source_capacity": self._namespace_dict("v2_source_capacity"),
            "inferred_source_capacity": self._namespace_dict(
                "inferred_source_capacity"
            ),
            "synthetic_arrival_capacity": self._namespace_dict(
                "synthetic_arrival_capacity"
            ),
        }

    def entry_counts(self) -> dict[str, int]:
        return {
            str(namespace): int(count)
            for namespace, count in self.connection.execute(
                "SELECT namespace, COUNT(*) FROM capacities GROUP BY namespace"
            )
        }

    def applied_batches(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM applied_deltas").fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.connection.close()

    def _namespace_dict(self, namespace: str) -> dict[str, str]:
        return dict(self._iter_namespace(namespace))

    def _iter_namespace(self, namespace: str) -> Iterator[tuple[str, str]]:
        for key, quantity in self.connection.execute(
            """
            SELECT capacity_key, quantity
            FROM capacities
            WHERE namespace = ?
            ORDER BY capacity_key
            """,
            (namespace,),
        ):
            yield str(key), str(quantity)

    def _hash_namespace_map(self, digest: Any, namespace: str) -> None:
        digest.update(b"{")
        first = True
        for key, quantity in self._iter_namespace(namespace):
            if not first:
                digest.update(b",")
            first = False
            digest.update(_json_string(key))
            digest.update(b":")
            digest.update(_json_string(quantity))
        digest.update(b"}")


def _delta_rows(
    execution_family: str, payload: Mapping[str, Any]
) -> Iterator[tuple[str, str, str]]:
    if execution_family == "V2":
        yield from _mapping_rows(
            "v2_source_capacity", payload.get("source_trade_consumed")
        )
        yield from _mapping_rows(
            "v2_market_window_capacity", payload.get("market_window_consumed")
        )
        return
    v2 = payload.get("v2")
    v2_payload = v2 if isinstance(v2, Mapping) else {}
    yield from _mapping_rows(
        "v2_source_capacity", v2_payload.get("source_trade_consumed")
    )
    yield from _mapping_rows(
        "v2_market_window_capacity", v2_payload.get("market_window_consumed")
    )
    yield from _mapping_rows(
        "inferred_source_capacity", payload.get("inferred_source_capacity")
    )
    yield from _mapping_rows(
        "synthetic_arrival_capacity", payload.get("synthetic_arrival_capacity")
    )


def _mapping_rows(
    namespace: str, value: Any
) -> Iterator[tuple[str, str, str]]:
    rows = value if isinstance(value, Mapping) else {}
    for key, quantity in rows.items():
        yield namespace, str(key), _decimal_text(quantity)


def _decimal_add(left: Any, right: Any) -> str:
    return _decimal_text(Decimal(str(left)) + Decimal(str(right)))


def _decimal_text(value: Any) -> str:
    return str(Decimal(str(value)).quantize(Q, rounding=ROUND_HALF_UP))


def _json_string(value: str) -> bytes:
    return json.dumps(str(value), separators=(",", ":")).encode()
