"""Shared PostgreSQL writer fence and target validation for block-close data.

Normal block-close ingestion/upsert transactions take the shared lock.  A
rewrite that must observe a stable block-close table (for example fair-price
recomputation) takes the exclusive lock.  Both locks are transaction-scoped,
so callers must acquire them on the exact connection that performs the write
and must do so before the first mutation in that transaction.

The advisory-lock key is a persistent coordination contract.  Do not change it
when moving code or renaming an entrypoint: independently deployed writers must
continue contending on the same PostgreSQL lock.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Final


BLOCK_CLOSE_WRITER_FENCE_KEY: Final[int] = 914_020_260_828
BLOCK_CLOSE_CANONICAL_TABLE: Final[str] = "quant.market_token_block_close"
BLOCK_CLOSE_REBUILD_TABLE_PATTERN: Final[str] = (
    r"^quant\.mtbc_rebuild_[0-9a-f]{32}$"
)
BLOCK_CLOSE_BACKUP_TABLE_PATTERN: Final[str] = r"^quant\.mtbc_backup_[0-9a-f]{32}$"
POSTGRES_IDENTIFIER_MAX_BYTES: Final[int] = 63
_BLOCK_CLOSE_REBUILD_TABLE_RE = re.compile(BLOCK_CLOSE_REBUILD_TABLE_PATTERN)
_BLOCK_CLOSE_BACKUP_TABLE_RE = re.compile(BLOCK_CLOSE_BACKUP_TABLE_PATTERN)


@dataclass(frozen=True, slots=True)
class BlockCloseTargetTable:
    """A validated, interpolation-safe PostgreSQL relation name."""

    schema_name: str
    table_name: str
    qualified_name: str
    is_canonical: bool


def _reject_oversized_identifiers(target: str) -> None:
    for identifier in target.split("."):
        if len(identifier.encode("utf-8")) > POSTGRES_IDENTIFIER_MAX_BYTES:
            raise ValueError(
                "block-close target identifier exceeds PostgreSQL's 63-byte limit"
            )


def resolve_block_close_target_table(value: str) -> BlockCloseTargetTable:
    """Validate a canonical or run-specific rebuild/backup target.

    Rebuild and backup targets must end in a lowercase UUID encoded as exactly
    32 hex characters. Returning fixed/regex-constrained identifiers keeps
    callers from interpolating arbitrary SQL identifiers.
    """

    target = str(value or "")
    _reject_oversized_identifiers(target)
    if target == BLOCK_CLOSE_CANONICAL_TABLE:
        table_name = "market_token_block_close"
        return BlockCloseTargetTable(
            schema_name="quant",
            table_name=table_name,
            qualified_name=f"quant.{table_name}",
            is_canonical=True,
        )
    if (
        _BLOCK_CLOSE_REBUILD_TABLE_RE.fullmatch(target)
        or _BLOCK_CLOSE_BACKUP_TABLE_RE.fullmatch(target)
    ):
        schema_name, table_name = target.split(".", 1)
        return BlockCloseTargetTable(
            schema_name=schema_name,
            table_name=table_name,
            qualified_name=f"{schema_name}.{table_name}",
            is_canonical=False,
        )
    raise ValueError(
        "block-close target must be quant.market_token_block_close or "
        "quant.mtbc_rebuild_<lowercase_uuid_hex32> or "
        "quant.mtbc_backup_<lowercase_uuid_hex32>"
    )


def validate_block_close_target_table(value: str) -> str:
    """Return the safe qualified identifier for an allowed target table."""

    return resolve_block_close_target_table(value).qualified_name


def acquire_block_close_writer_shared(conn: Any) -> None:
    """Join the normal block-close writer cohort for the current transaction."""

    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock_shared(%s)",
            (BLOCK_CLOSE_WRITER_FENCE_KEY,),
        )


def acquire_block_close_writer_exclusive(conn: Any) -> None:
    """Exclude all participating block-close writers for this transaction."""

    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (BLOCK_CLOSE_WRITER_FENCE_KEY,),
        )
