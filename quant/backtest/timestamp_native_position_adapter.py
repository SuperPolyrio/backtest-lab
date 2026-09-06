"""Export completed timestamp-native Fill-only results as generic positions."""

from __future__ import annotations

import gzip
import json
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

from .fill_only_financials import (
    FillOnlyExecutionPosition,
    FillOnlyFinancialError,
    write_fill_only_execution_positions,
)
from .market_settlement import file_sha256

UTC = timezone.utc
ENGINE_QUANTUM = Decimal("0.0000000001")


class TimestampNativePositionAdapterError(FillOnlyFinancialError):
    """Frozen timestamp-native execution evidence is incomplete or drifted."""


def _canonical_sha256(value: object) -> str:
    return sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _read_object(path: Path, *, label: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        raise TimestampNativePositionAdapterError(
            f"{label} must be a regular non-symlink file"
        )
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TimestampNativePositionAdapterError(f"{label} must be a JSON object")
    return value


def _verify_self_hash(
    payload: Mapping[str, object], hash_field: str, *, label: str
) -> None:
    claimed = payload.get(hash_field)
    actual = _canonical_sha256(
        {key: value for key, value in payload.items() if key != hash_field}
    )
    if claimed != actual:
        raise TimestampNativePositionAdapterError(f"{label} self-hash drifted")


def _utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise TimestampNativePositionAdapterError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TimestampNativePositionAdapterError(
            f"{field} must be an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TimestampNativePositionAdapterError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _asset_from_order_id(order_id: str) -> str:
    parts = order_id.rsplit(":", 2)
    if len(parts) != 3 or parts[-1].lower() not in {"buy", "sell"}:
        raise TimestampNativePositionAdapterError(
            f"cannot resolve asset id from order {order_id}"
        )
    asset = parts[-2].lower().removeprefix("0x")
    if len(asset) != 64 or any(char not in "0123456789abcdef" for char in asset):
        raise TimestampNativePositionAdapterError(
            f"order {order_id} does not contain a 32-byte asset id"
        )
    return asset


def _decimal_token(asset_id: str) -> str:
    return str(int(asset_id, 16))


def _checkpoint_result_file(
    root: Path,
    *,
    day: str,
    profile: str,
    execution_manifest_sha256: str,
    previous_checkpoint_sha256: str,
) -> tuple[Path, dict[str, object], str]:
    day_root = root / "days" / day
    checkpoint = _read_object(day_root / "checkpoint.json", label=f"{day} checkpoint")
    _verify_self_hash(checkpoint, "day_checkpoint_sha256", label=f"{day} checkpoint")
    if checkpoint.get("signal_day") != day:
        raise TimestampNativePositionAdapterError("day checkpoint identity drifted")
    if checkpoint.get("execution_manifest_sha256") != execution_manifest_sha256:
        raise TimestampNativePositionAdapterError(
            "day checkpoint uses a different execution manifest"
        )
    if checkpoint.get("previous_day_checkpoint_sha256") != previous_checkpoint_sha256:
        raise TimestampNativePositionAdapterError("day checkpoint chain drifted")
    profile_results = checkpoint.get("profile_results")
    if not isinstance(profile_results, Mapping):
        raise TimestampNativePositionAdapterError("checkpoint lacks profile_results")
    metadata = profile_results.get(profile)
    if not isinstance(metadata, Mapping):
        raise TimestampNativePositionAdapterError(
            f"checkpoint does not contain profile {profile}"
        )
    relative = Path(str(metadata.get("file") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise TimestampNativePositionAdapterError("unsafe profile result path")
    result_path = (day_root / relative).resolve()
    if not result_path.is_file() or result_path.is_symlink():
        raise TimestampNativePositionAdapterError("profile result file is missing")
    if root not in result_path.parents:
        raise TimestampNativePositionAdapterError("profile result escaped execution root")
    if file_sha256(result_path) != metadata.get("file_sha256"):
        raise TimestampNativePositionAdapterError("profile result file hash drifted")
    return result_path, dict(metadata), str(checkpoint["day_checkpoint_sha256"])


def _condensed_positive_result(
    row: Mapping[str, object],
    *,
    profile: str,
    day: str,
) -> dict[str, object] | None:
    if row.get("schema_version") != "PMQ073TimestampNativeMatcherResultV2":
        raise TimestampNativePositionAdapterError("unsupported matcher result schema")
    if row.get("profile") != profile or row.get("signal_day") != day:
        raise TimestampNativePositionAdapterError("matcher result scope drifted")
    matcher = row.get("matcher_result")
    if not isinstance(matcher, Mapping):
        raise TimestampNativePositionAdapterError("matcher result payload is missing")
    status = str(matcher.get("status") or "")
    filled_size = Decimal(str(matcher.get("filled_size") or 0))
    filled_notional = Decimal(str(matcher.get("filled_notional") or 0))
    fills = matcher.get("fills")
    if status in {"NO_FILL", "REJECTED"}:
        if filled_size != 0 or filled_notional != 0 or fills not in (None, (), []):
            raise TimestampNativePositionAdapterError(
                "unfilled matcher result carries positive economics"
            )
        return None
    if status not in {"FILLED", "PARTIAL_FILLED"}:
        raise TimestampNativePositionAdapterError(f"unsupported matcher status {status}")
    if str(matcher.get("side") or "").upper() != "BUY":
        raise TimestampNativePositionAdapterError(
            "generic positive-position export currently requires BUY fills"
        )
    if (
        filled_size <= 0
        or filled_notional <= 0
        or not isinstance(fills, Sequence)
        or isinstance(fills, (str, bytes))
        or not fills
    ):
        raise TimestampNativePositionAdapterError(
            "filled matcher result lacks positive fill evidence"
        )
    normalized_fills: list[tuple[datetime, int, Decimal, Decimal, str]] = []
    for fill in fills:
        if not isinstance(fill, Mapping):
            raise TimestampNativePositionAdapterError("matcher fill must be an object")
        fill_size = Decimal(str(fill.get("filled_size") or 0))
        exec_price = Decimal(str(fill.get("exec_price") or 0))
        fill_block = int(str(fill.get("fill_block") or 0))
        source_id = str(fill.get("source_trade_id") or "")
        if fill_size <= 0 or not (Decimal(0) <= exec_price <= Decimal(1)):
            raise TimestampNativePositionAdapterError("matcher fill economics are invalid")
        if fill_block <= 0 or not source_id:
            raise TimestampNativePositionAdapterError("matcher fill source is incomplete")
        normalized_fills.append(
            (
                _utc(fill.get("fill_ts"), field="fill_ts"),
                fill_block,
                fill_size,
                exec_price,
                source_id,
            )
        )
    normalized_fills.sort(key=lambda item: (item[0], item[1], item[4]))
    reconstructed_size = sum((item[2] for item in normalized_fills), Decimal(0))
    reconstructed_notional = sum(
        (item[2] * item[3] for item in normalized_fills), Decimal(0)
    ).quantize(ENGINE_QUANTUM, rounding=ROUND_HALF_UP)
    if reconstructed_size != filled_size or reconstructed_notional != filled_notional:
        raise TimestampNativePositionAdapterError(
            "matcher aggregate economics differ from fill evidence"
        )
    order_id = str(matcher.get("order_id") or "")
    asset_id = _asset_from_order_id(order_id)
    return {
        "position_id": order_id,
        "asset_id": asset_id,
        "filled_size": str(filled_size),
        "entry_notional": str(filled_notional),
        "first_fill_ts": normalized_fills[0][0].isoformat(),
        "last_fill_ts": normalized_fills[-1][0].isoformat(),
        # Match-time ordering can differ from settlement block ordering.  The
        # full block span is the conservative evidence needed by finalization.
        "first_fill_block": min(item[1] for item in normalized_fills),
        "last_fill_block": max(item[1] for item in normalized_fills),
        "source_fill_count": len(normalized_fills),
        "source_result_sha256": _canonical_sha256(row),
        "global_order_ordinal": int(str(row.get("global_order_ordinal"))),
    }


def _token_mappings(
    conn: Any, token_ids: Sequence[str], *, chunk_size: int = 5_000
) -> dict[str, dict[str, object]]:
    mappings: dict[str, set[tuple[int, str, str, str]]] = {}
    query = """
        WITH requested AS (
            SELECT unnest(%s::text[]) AS token_id
        ), candidates AS (
            SELECT mt.token_id, mt.market_id, lower(mt.condition_id) AS condition_id,
                   upper(mt.outcome) AS outcome, 'core.market_tokens' AS source
            FROM core.market_tokens mt
            WHERE mt.token_id = ANY(%s::text[])
            UNION ALL
            SELECT m.yes_token_id, m.id, lower(m.condition_id), 'YES',
                   'core.markets_shortcut_tokens'
            FROM core.markets m
            WHERE m.yes_token_id = ANY(%s::text[])
            UNION ALL
            SELECT m.no_token_id, m.id, lower(m.condition_id), 'NO',
                   'core.markets_shortcut_tokens'
            FROM core.markets m
            WHERE m.no_token_id = ANY(%s::text[])
        )
        SELECT r.token_id, c.market_id, c.condition_id, c.outcome, c.source
        FROM requested r
        LEFT JOIN candidates c ON c.token_id=r.token_id
        ORDER BY r.token_id, c.market_id, c.outcome, c.source
    """
    for offset in range(0, len(token_ids), chunk_size):
        chunk = list(token_ids[offset : offset + chunk_size])
        with conn.cursor() as cursor:
            cursor.execute(query, (chunk, chunk, chunk, chunk))
            rows = [dict(item) for item in cursor.fetchall()]
        for row in rows:
            token_id = str(row["token_id"])
            if row.get("market_id") is None:
                mappings.setdefault(token_id, set())
                continue
            candidate = (
                int(row["market_id"]),
                str(row["condition_id"]),
                str(row["outcome"]),
                str(row["source"]),
            )
            mappings.setdefault(token_id, set()).add(candidate)
    resolved: dict[str, dict[str, object]] = {}
    for token_id in token_ids:
        resolved[token_id] = _resolve_token_candidates(
            token_id, mappings.get(token_id, set())
        )
    return resolved


def _resolve_token_candidates(
    token_id: str,
    candidates: set[tuple[int, str, str, str]],
) -> dict[str, object]:
    authoritative = {
        item for item in candidates if item[3] == "core.market_tokens"
    }
    effective = authoritative or candidates
    identities = {(item[0], item[1], item[2]) for item in effective}
    if len(identities) != 1:
        raise TimestampNativePositionAdapterError(
            f"token {token_id} has {len(identities)} authoritative market identities"
        )
    market_id, condition_id, outcome = next(iter(identities))
    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "outcome": outcome,
        "mapping_sources": tuple(sorted(item[3] for item in effective)),
    }


def _iter_staged_positions(
    stage_path: Path,
    *,
    execution_run_id: str,
    profile: str,
    token_mappings: Mapping[str, Mapping[str, object]],
) -> Iterator[FillOnlyExecutionPosition]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(stage_path)
    for batch in parquet.iter_batches(batch_size=10_000):
        for row in batch.to_pylist():
            asset_id = str(row["asset_id"])
            mapping = token_mappings[_decimal_token(asset_id)]
            yield FillOnlyExecutionPosition(
                execution_run_id=execution_run_id,
                profile=profile,
                position_id=str(row["position_id"]),
                market_id=int(mapping["market_id"]),
                condition_id=str(mapping["condition_id"]),
                asset_id=asset_id,
                outcome=str(mapping["outcome"]),
                filled_size=Decimal(str(row["filled_size"])),
                entry_notional=Decimal(str(row["entry_notional"])),
                fee_paid=Decimal(0),
                first_fill_ts=_utc(row["first_fill_ts"], field="first_fill_ts"),
                last_fill_ts=_utc(row["last_fill_ts"], field="last_fill_ts"),
                first_fill_block=int(row["first_fill_block"]),
                last_fill_block=int(row["last_fill_block"]),
                source_order_ids=(str(row["position_id"]),),
                source_fill_ids=(),
                execution_evidence_sha256=str(row["source_result_sha256"]),
            )


def export_timestamp_native_execution_positions(
    conn: Any,
    *,
    execution_root: Path,
    profile: str,
    output_dir: Path,
) -> dict[str, object]:
    """Verify a completed run and export only its positive BUY inventory."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    root = execution_root.resolve()
    manifest_path = root / "execution_manifest.json"
    state_path = root / "execution_state.json"
    summaries_path = root / "profile_summaries.json"
    manifest = _read_object(manifest_path, label="execution manifest")
    state = _read_object(state_path, label="execution state")
    summaries = _read_object(summaries_path, label="profile summaries")
    _verify_self_hash(state, "state_sha256", label="execution state")
    if state.get("status") != "COMPLETE":
        raise TimestampNativePositionAdapterError("execution state is not COMPLETE")
    execution_manifest_sha = str(state.get("execution_manifest_sha256") or "")
    if execution_manifest_sha != manifest.get("execution_manifest_sha256"):
        raise TimestampNativePositionAdapterError(
            "execution state and manifest identity differ"
        )
    if summaries.get("execution_manifest_sha256") != execution_manifest_sha:
        raise TimestampNativePositionAdapterError(
            "profile summaries use a different execution manifest"
        )
    profiles = summaries.get("profiles")
    profile_summary = profiles.get(profile) if isinstance(profiles, Mapping) else None
    if not isinstance(profile_summary, Mapping):
        raise TimestampNativePositionAdapterError(f"unknown execution profile {profile}")
    summary = profile_summary.get("summary")
    if not isinstance(summary, Mapping):
        raise TimestampNativePositionAdapterError("profile financial summary is missing")
    completed_days = state.get("completed_days")
    if not isinstance(completed_days, Sequence) or isinstance(completed_days, str):
        raise TimestampNativePositionAdapterError("execution state day inventory is invalid")

    output_parent = output_dir.resolve().parent
    output_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="fill-only-position-stage-", dir=output_parent
    ) as temporary_dir:
        stage_path = Path(temporary_dir) / "positive_results.parquet"
        stage_writer: pq.ParquetWriter | None = None
        stage_rows: list[dict[str, object]] = []
        token_ids: set[str] = set()
        result_file_chain = "0" * 64
        positive_count = 0
        total_result_count = 0
        positive_fill_count = 0
        total_size = Decimal(0)
        total_notional = Decimal(0)
        previous_checkpoint = "0" * 64

        def flush_stage() -> None:
            nonlocal stage_writer
            if not stage_rows:
                return
            table = pa.Table.from_pylist(stage_rows)
            if stage_writer is None:
                stage_writer = pq.ParquetWriter(
                    stage_path, table.schema, compression="zstd", use_dictionary=True
                )
            stage_writer.write_table(table, row_group_size=len(stage_rows))
            stage_rows.clear()

        try:
            for day_value in completed_days:
                day = str(day_value)
                result_path, metadata, previous_checkpoint = _checkpoint_result_file(
                    root,
                    day=day,
                    profile=profile,
                    execution_manifest_sha256=execution_manifest_sha,
                    previous_checkpoint_sha256=previous_checkpoint,
                )
                result_file_chain = sha256(
                    bytes.fromhex(result_file_chain)
                    + bytes.fromhex(str(metadata["file_sha256"]))
                ).hexdigest()
                day_count = 0
                with gzip.open(result_path, "rt", encoding="utf-8") as stream:
                    for line in stream:
                        if not line.strip():
                            continue
                        raw = json.loads(line)
                        if not isinstance(raw, Mapping):
                            raise TimestampNativePositionAdapterError(
                                "matcher result row must be an object"
                            )
                        day_count += 1
                        condensed = _condensed_positive_result(
                            raw, profile=profile, day=day
                        )
                        if condensed is None:
                            continue
                        positive_count += 1
                        positive_fill_count += int(condensed["source_fill_count"])
                        total_size += Decimal(str(condensed["filled_size"]))
                        total_notional += Decimal(str(condensed["entry_notional"]))
                        asset_id = str(condensed["asset_id"])
                        token_ids.add(_decimal_token(asset_id))
                        stage_rows.append(condensed)
                        if len(stage_rows) >= 10_000:
                            flush_stage()
                if day_count != int(metadata["result_count"]):
                    raise TimestampNativePositionAdapterError(
                        f"{day} result count differs from checkpoint"
                    )
                total_result_count += day_count
            flush_stage()
        finally:
            if stage_writer is not None:
                stage_writer.close()

        if not stage_path.is_file():
            raise TimestampNativePositionAdapterError(
                "selected execution profile has no positive positions"
            )
        if previous_checkpoint != state.get("last_day_checkpoint_sha256"):
            raise TimestampNativePositionAdapterError(
                "execution state does not point to the final day checkpoint"
            )
        expected_positive = int(summary["filled_orders"]) + int(
            summary["partial_orders"]
        )
        if (
            total_result_count != int(summary["attempted_orders"])
            or positive_count != expected_positive
            or total_size != Decimal(str(summary["filled_size"]))
            or total_notional != Decimal(str(summary["filled_notional"]))
            or positive_fill_count != int(summary["source_fill_count"])
        ):
            raise TimestampNativePositionAdapterError(
                "exported positive inventory differs from frozen profile summary"
            )
        mappings = _token_mappings(conn, tuple(sorted(token_ids)))
        execution_source = {
            "adapter": "TIMESTAMP_NATIVE_FILL_ONLY_POSITIVE_BUY_V1",
            "execution_root": str(root),
            "profile": profile,
            "execution_manifest_sha256": execution_manifest_sha,
            "execution_manifest_file_sha256": file_sha256(manifest_path),
            "execution_state_file_sha256": file_sha256(state_path),
            "profile_summaries_file_sha256": file_sha256(summaries_path),
            "result_file_hash_chain_sha256": result_file_chain,
            "result_count": total_result_count,
            "positive_position_count": positive_count,
            "source_fill_count": positive_fill_count,
            "filled_size": str(total_size),
            "filled_notional": str(total_notional),
            "source_fill_ids_preserved_in_original_execution": True,
            "source_fill_ids_duplicated_into_position_artifact": False,
        }
        return write_fill_only_execution_positions(
            _iter_staged_positions(
                stage_path,
                execution_run_id=execution_manifest_sha,
                profile=profile,
                token_mappings=mappings,
            ),
            output_dir=output_dir,
            execution_source=execution_source,
        )
