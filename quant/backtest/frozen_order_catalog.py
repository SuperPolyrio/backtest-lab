"""Parquet-backed immutable order inventory for profile workers."""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import shutil
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from quant.backtest.orderfilled_v2_replay import V2TakerOrder
from quant.backtest.trade_only_v3.models import LiquidityIntent, TradeOnlyOrder

ORDER_CATALOG_SCHEMA_VERSION = "UnifiedFillOnlyFrozenOrdersV2"
LEGACY_ORDER_CATALOG_SCHEMA_VERSION = "UnifiedFillOnlyFrozenOrdersV1"
ORDER_CATALOG_CHECKPOINT_VERSION = "UnifiedFillOnlyFrozenOrdersCheckpointV1"
ORDER_SCHEMA = pa.schema(
    [
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("execution_family", pa.string(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("market_id", pa.uint64(), nullable=False),
        pa.field("asset_id", pa.string(), nullable=False),
        pa.field("signal_block", pa.int64()),
        pa.field("signal_ts", pa.timestamp("us", tz="UTC")),
        pa.field("payload_json", pa.string(), nullable=False),
    ]
)


class FrozenOrderCatalog:
    def __init__(
        self,
        root: str | Path,
        *,
        source_pin: str | None = None,
        strategy_hash: str | None = None,
        verify_checksums: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        self.manifest = json.loads(
            (self.root / "manifest.json").read_text(encoding="utf-8")
        )
        _verify_manifest(self.manifest)
        if source_pin is not None and self.manifest["source_pin"] != source_pin:
            raise ValueError("frozen order source_pin mismatch")
        if strategy_hash is not None and self.manifest["strategy_hash"] != strategy_hash:
            raise ValueError("frozen order strategy_hash mismatch")
        self._files: tuple[dict[str, Any], ...]
        if self.manifest["schema_version"] == LEGACY_ORDER_CATALOG_SCHEMA_VERSION:
            self._files = (
                {
                    "path": str(self.manifest["file"]),
                    "rows": int(self.manifest["rows"]),
                    "sha256": str(self.manifest["file_sha256"]),
                },
            )
        else:
            self._files = tuple(
                cast(dict[str, Any], dict(item)) for item in self.manifest["files"]
            )
        self._paths = tuple(self.root / str(item["path"]) for item in self._files)
        if verify_checksums:
            for item, path in zip(self._files, self._paths, strict=True):
                if _file_sha256(path) != item["sha256"]:
                    raise ValueError(
                        f"frozen order file checksum mismatch: {item['path']}"
                    )

    @property
    def execution_family(self) -> str:
        return str(self.manifest["execution_family"])

    @property
    def rows(self) -> int:
        return int(self.manifest["rows"])

    @property
    def strategy_hash(self) -> str:
        return str(self.manifest["strategy_hash"])

    def iter_batches(
        self, *, batch_size: int = 10_000, start_offset: int = 0
    ) -> Iterator[list[Any]]:
        """Read from an inventory cursor without decoding prior orders."""

        remaining_skip = int(start_offset)
        if remaining_skip < 0:
            raise ValueError("start_offset cannot be negative")
        for item, path in zip(self._files, self._paths, strict=True):
            file_rows = int(item["rows"])
            if remaining_skip >= file_rows:
                remaining_skip -= file_rows
                continue
            parquet = pq.ParquetFile(path, memory_map=True)
            for batch in parquet.iter_batches(
                batch_size=max(1, int(batch_size)), columns=ORDER_SCHEMA.names
            ):
                if remaining_skip >= batch.num_rows:
                    remaining_skip -= batch.num_rows
                    continue
                if remaining_skip:
                    batch = batch.slice(remaining_skip)
                    remaining_skip = 0
                rows = pa.Table.from_batches([batch], schema=ORDER_SCHEMA).to_pylist()
                yield [
                    _deserialize_order(json.loads(row["payload_json"])) for row in rows
                ]

    def iter_signal_day_batches(
        self,
        *,
        read_batch_size: int = 10_000,
        days_per_batch: int = 1,
        start_offset: int = 0,
    ) -> Iterator[list[Any]]:
        """Yield chronological orders without splitting one UTC signal day."""

        if days_per_batch <= 0:
            raise ValueError("days_per_batch must be positive")
        grouped: list[Any] = []
        grouped_days = 0
        for day_batch in self._iter_single_signal_day_batches(
            read_batch_size=read_batch_size,
            start_offset=start_offset,
        ):
            grouped.extend(day_batch)
            grouped_days += 1
            if grouped_days >= days_per_batch:
                yield grouped
                grouped = []
                grouped_days = 0
        if grouped:
            yield grouped

    def _iter_single_signal_day_batches(
        self, *, read_batch_size: int, start_offset: int = 0
    ) -> Iterator[list[Any]]:

        buffered: list[Any] = []
        current_day: Any = None
        previous_day: Any = None
        for batch in self.iter_batches(
            batch_size=read_batch_size,
            start_offset=start_offset,
        ):
            for order in batch:
                signal_ts = getattr(order, "signal_ts", None)
                day = _utc(signal_ts).date() if signal_ts is not None else None
                if previous_day is not None and day is not None and day < previous_day:
                    raise ValueError("frozen order catalog is not chronological by signal day")
                if buffered and day != current_day:
                    yield buffered
                    buffered = []
                buffered.append(order)
                current_day = day
                if day is not None:
                    previous_day = day
        if buffered:
            yield buffered


class FrozenOrderCatalogBuilder:
    """Stream immutable orders into resumable multipart Parquet files."""

    def __init__(
        self,
        root: str | Path,
        *,
        source_pin: str,
        strategy_hash: str,
        row_group_size: int = 10_000,
        resume: bool = False,
    ) -> None:
        for name, value in (("source_pin", source_pin), ("strategy_hash", strategy_hash)):
            if not str(value).strip():
                raise ValueError(f"{name} is required")
        self.root = Path(root).resolve()
        if self.root.exists():
            raise FileExistsError(f"frozen order catalog exists: {self.root}")
        self.source_pin = str(source_pin)
        self.strategy_hash = str(strategy_hash)
        self.row_group_size = max(1, int(row_group_size))
        self.resume = bool(resume)
        self.staging = (
            self.root.with_name(f".{self.root.name}.building")
            if self.resume
            else self.root.with_name(f".{self.root.name}.tmp-{uuid.uuid4().hex}")
        )
        self.checkpoint_path = self.staging / "builder_checkpoint.json"
        self._files: list[dict[str, Any]] = []
        self._rows = 0
        self._part = 0
        self._family: str | None = None
        self._closed = False
        if self.staging.exists():
            if not self.resume:
                raise FileExistsError(f"frozen order staging exists: {self.staging}")
            self._restore_checkpoint()
        else:
            self.staging.mkdir(parents=True)
            self._write_checkpoint()

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def execution_family(self) -> str | None:
        return self._family

    def append(self, orders: Iterable[V2TakerOrder | TradeOnlyOrder]) -> int:
        if self._closed:
            raise RuntimeError("frozen order catalog builder is closed")
        batch = list(orders)
        if not batch:
            self._write_checkpoint()
            return 0
        family = "V2" if isinstance(batch[0], V2TakerOrder) else "V3"
        expected_type = V2TakerOrder if family == "V2" else TradeOnlyOrder
        if not all(isinstance(order, expected_type) for order in batch):
            raise TypeError("frozen order catalog cannot mix V2 and V3")
        if self._family is not None and self._family != family:
            raise TypeError("frozen order catalog cannot mix V2 and V3")
        self._family = family
        payloads = [_serialize_order(order) for order in batch]
        start_sequence = self._rows
        table = pa.Table.from_pydict(
            {
                "sequence": list(range(start_sequence, start_sequence + len(batch))),
                "execution_family": [family] * len(batch),
                "order_id": [order.order_id for order in batch],
                "market_id": [order.market_id for order in batch],
                "asset_id": [order.asset_id.lower() for order in batch],
                "signal_block": [getattr(order, "signal_block", None) for order in batch],
                "signal_ts": [
                    _utc(value) if value is not None else None
                    for value in (getattr(order, "signal_ts", None) for order in batch)
                ],
                "payload_json": [_canonical_json(payload) for payload in payloads],
            },
            schema=ORDER_SCHEMA,
        )
        relative = Path("parts") / f"part-{self._part:08d}.parquet"
        target = self.staging / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".parquet.tmp")
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            row_group_size=self.row_group_size,
            write_statistics=True,
        )
        os.replace(temporary, target)
        receipt = {
            "path": relative.as_posix(),
            "rows": len(batch),
            "first_sequence": start_sequence,
            "last_sequence": start_sequence + len(batch) - 1,
            "sha256": _file_sha256(target),
            "payload_sha256": _line_stream_sha256(payloads),
        }
        self._files.append(receipt)
        self._part += 1
        self._rows += len(batch)
        self._write_checkpoint()
        return len(batch)

    def finalize(self) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("frozen order catalog builder is closed")
        if self._rows <= 0 or self._family is None:
            raise ValueError("frozen order catalog requires at least one order")
        draft = {
            "schema_version": ORDER_CATALOG_SCHEMA_VERSION,
            "source_pin": self.source_pin,
            "strategy_hash": self.strategy_hash,
            "execution_family": self._family,
            "rows": self._rows,
            "orders_sha256": _canonical_sha256(
                [str(item["payload_sha256"]) for item in self._files]
            ),
            "arrow_schema_sha256": hashlib.sha256(
                ORDER_SCHEMA.serialize().to_pybytes()
            ).hexdigest(),
            "files": list(self._files),
        }
        manifest = {**draft, "manifest_sha256": _canonical_sha256(draft)}
        temporary = self.staging / "manifest.json.tmp"
        temporary.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, self.staging / "manifest.json")
        self.checkpoint_path.unlink(missing_ok=True)
        self.root.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.staging, self.root)
        self._closed = True
        return manifest

    def abort(self, *, preserve_resume_state: bool = True) -> None:
        if self.staging.exists() and not (self.resume and preserve_resume_state):
            shutil.rmtree(self.staging)
        self._closed = True

    def _write_checkpoint(self) -> None:
        draft = {
            "schema_version": ORDER_CATALOG_CHECKPOINT_VERSION,
            "source_pin": self.source_pin,
            "strategy_hash": self.strategy_hash,
            "arrow_schema_sha256": hashlib.sha256(
                ORDER_SCHEMA.serialize().to_pybytes()
            ).hexdigest(),
            "row_group_size": self.row_group_size,
            "execution_family": self._family,
            "rows": self._rows,
            "next_part": self._part,
            "files": self._files,
        }
        _atomic_json(
            self.checkpoint_path,
            {**draft, "checkpoint_sha256": _canonical_sha256(draft)},
        )

    def _restore_checkpoint(self) -> None:
        if not self.checkpoint_path.exists():
            raise ValueError("resumable frozen order staging has no checkpoint")
        payload = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        digest = str(payload.pop("checkpoint_sha256", ""))
        if digest != _canonical_sha256(payload):
            raise ValueError("frozen order builder checkpoint checksum mismatch")
        for key, expected in (
            ("schema_version", ORDER_CATALOG_CHECKPOINT_VERSION),
            ("source_pin", self.source_pin),
            ("strategy_hash", self.strategy_hash),
            ("row_group_size", self.row_group_size),
            (
                "arrow_schema_sha256",
                hashlib.sha256(ORDER_SCHEMA.serialize().to_pybytes()).hexdigest(),
            ),
        ):
            if payload.get(key) != expected:
                raise ValueError(f"frozen order builder checkpoint {key} mismatch")
        self._files = [dict(item) for item in payload["files"]]
        self._rows = int(payload["rows"])
        self._part = int(payload["next_part"])
        self._family = payload.get("execution_family")
        for item in self._files:
            path = self.staging / str(item["path"])
            if _file_sha256(path) != item["sha256"]:
                raise ValueError(f"frozen order checkpoint file mismatch: {item['path']}")
        referenced = {str(item["path"]) for item in self._files}
        for path in (self.staging / "parts").glob("part-*.parquet"):
            if path.relative_to(self.staging).as_posix() not in referenced:
                path.unlink()


def write_frozen_order_catalog(
    root: str | Path,
    orders: Iterable[V2TakerOrder | TradeOnlyOrder],
    *,
    source_pin: str,
    strategy_hash: str,
    row_group_size: int = 10_000,
    resume: bool = False,
) -> dict[str, Any]:
    builder = FrozenOrderCatalogBuilder(
        root,
        source_pin=source_pin,
        strategy_hash=strategy_hash,
        row_group_size=row_group_size,
        resume=resume,
    )
    try:
        iterator = iter(orders)
        while batch := list(itertools.islice(iterator, max(1, int(row_group_size)))):
            builder.append(batch)
        return builder.finalize()
    except Exception:
        builder.abort(preserve_resume_state=True)
        raise


def v2_order_to_trade_only_template(order: V2TakerOrder) -> TradeOnlyOrder:
    """Preserve strategy intent while deferring execution defaults to a V3 profile."""

    if order.signal_ts is None:
        raise ValueError(f"V2 order {order.order_id!r} has no signal_ts")
    seed = int.from_bytes(
        hashlib.sha256(order.order_id.encode("utf-8")).digest()[:8], "big"
    ) & (2**63 - 1)
    return TradeOnlyOrder(
        order_id=order.order_id,
        market_id=order.market_id,
        asset_id=order.asset_id,
        side=order.side,
        limit_price=order.limit_price,
        size=order.size,
        signal_block=order.signal_block,
        signal_ts=order.signal_ts,
        tif=order.tif,
        liquidity_intent=LiquidityIntent.TAKER,
        latency=order.latency,
        latency_blocks=order.latency_blocks,
        horizon=order.horizon or timedelta(minutes=5),
        horizon_blocks=order.horizon_blocks or 2_000,
        lookback=(
            order.trade_slice_lookback
            if order.trade_slice_lookback > timedelta(0)
            else timedelta(minutes=5)
        ),
        lookback_blocks=max(300, int(order.trade_slice_lookback_blocks)),
        allow_partial_fill=order.allow_partial_fill,
        signal_source_trade_id=order.signal_source_trade_id,
        random_seed=seed,
    )


def convert_v2_frozen_order_catalog_to_v3(
    source_root: str | Path,
    target_root: str | Path,
    *,
    row_group_size: int = 10_000,
) -> dict[str, Any]:
    """Stream a timestamp-native V2 inventory into a profile-bound V3 template."""

    source = FrozenOrderCatalog(source_root)
    if source.execution_family != "V2":
        raise ValueError("source frozen order catalog must use execution family V2")
    contract = {
        "schema_version": "V2ToV3FrozenOrderConversionV1",
        "source_strategy_hash": source.strategy_hash,
        "liquidity_intent": LiquidityIntent.TAKER.value,
        "profile_defaults_applied_at_replay": True,
        "random_seed": "sha256(order_id)[0:8]-big-endian",
    }
    strategy_hash = _canonical_sha256(contract)
    builder = FrozenOrderCatalogBuilder(
        target_root,
        source_pin=str(source.manifest["source_pin"]),
        strategy_hash=strategy_hash,
        row_group_size=row_group_size,
    )
    try:
        for batch in source.iter_batches(batch_size=row_group_size):
            builder.append(
                [
                    v2_order_to_trade_only_template(order)
                    for order in batch
                    if isinstance(order, V2TakerOrder)
                ]
            )
        manifest = builder.finalize()
    except Exception:
        builder.abort(preserve_resume_state=False)
        raise
    return {"contract": contract, "manifest": manifest}


def _serialize_order(order: V2TakerOrder | TradeOnlyOrder) -> dict[str, Any]:
    return {
        "type": type(order).__name__,
        "fields": {
            item.name: _json_value(getattr(order, item.name)) for item in fields(order)
        },
    }


def _deserialize_order(payload: dict[str, Any]) -> V2TakerOrder | TradeOnlyOrder:
    order_type = str(payload["type"])
    values = dict(payload["fields"])
    if order_type == "V2TakerOrder":
        for key in ("signal_ts",):
            values[key] = _datetime_value(values.get(key))
        for key in ("latency", "horizon", "trade_slice_lookback", "quote_proxy_ttl"):
            values[key] = _timedelta_value(values.get(key))
        for key in (
            "limit_price",
            "size",
            "participation_rate",
            "price_buffer",
            "min_trailing_same_side_volume",
            "trailing_volume_multiplier",
            "trailing_participation_rate",
            "max_fill_size_per_order",
            "market_window_cap",
            "min_future_eligible_volume",
        ):
            values[key] = _decimal_value(values.get(key))
        values["signal_source_log_indexes"] = tuple(
            int(value) for value in values.get("signal_source_log_indexes") or ()
        )
        return V2TakerOrder(**values)
    if order_type == "TradeOnlyOrder":
        for key in ("signal_ts", "market_end_ts"):
            values[key] = _datetime_value(values.get(key))
        for key in ("latency", "horizon", "lookback"):
            values[key] = _timedelta_value(values.get(key))
        for key in ("limit_price", "size"):
            values[key] = _decimal_value(values.get(key))
        values["liquidity_intent"] = LiquidityIntent(values["liquidity_intent"])
        return TradeOnlyOrder(**values)
    raise ValueError(f"unsupported frozen order type: {order_type}")


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": _utc(value).isoformat()}
    if isinstance(value, timedelta):
        return {"__type__": "timedelta", "value": str(value.total_seconds())}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None
    return _utc(datetime.fromisoformat(str(value["value"]).replace("Z", "+00:00")))


def _timedelta_value(value: Any) -> timedelta | None:
    if value is None:
        return None
    return timedelta(seconds=float(value["value"]))


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value["value"]))


def _verify_manifest(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") not in {
        ORDER_CATALOG_SCHEMA_VERSION,
        LEGACY_ORDER_CATALOG_SCHEMA_VERSION,
    }:
        raise ValueError("unsupported frozen order catalog schema")
    expected = str(payload.get("manifest_sha256") or "")
    draft = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    if expected != _canonical_sha256(draft):
        raise ValueError("frozen order manifest checksum mismatch")
    schema_hash = hashlib.sha256(ORDER_SCHEMA.serialize().to_pybytes()).hexdigest()
    if payload.get("arrow_schema_sha256") != schema_hash:
        raise ValueError("frozen order Arrow schema mismatch")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _line_stream_sha256(payloads: Iterable[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for payload in payloads:
        digest.update(_canonical_json(payload).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
