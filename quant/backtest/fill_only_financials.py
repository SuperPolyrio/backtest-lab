"""Strategy-independent financial finalization for positive Fill-only positions."""

from __future__ import annotations

import json
import math
import shutil
import statistics
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from .market_settlement import (
    EXACT_CANONICAL_BLOCK_HEADER_GRADE,
    IDENTITY_CONFLICT,
    ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE,
    ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
    PAYABLE_CLASSIFICATIONS,
    POST_CUTOFF,
    TECHNICAL_MISSING,
    UNRESOLVED,
    MarketSettlementRecord,
    file_sha256,
)

UTC = timezone.utc
POSITION_SCHEMA_VERSION = "fill_only_execution_position_v1"
SETTLED_POSITION_SCHEMA_VERSION = "fill_only_financial_position_v1"
FINANCIAL_SUMMARY_SCHEMA_VERSION = "fill_only_financial_summary_v1"
FEE_SCENARIO_BPS = (0, 25, 50, 100)
FORMAL_EXACT_HEADER = "exact-header"
NORMAL_ORACLE_FINALIZED = "oracle-finalized"
RESEARCH_CUTOFF_BLOCK = "oracle-event-cutoff-block"
FINANCIAL_EVIDENCE_REQUIREMENTS = {
    FORMAL_EXACT_HEADER,
    NORMAL_ORACLE_FINALIZED,
    RESEARCH_CUTOFF_BLOCK,
}


class FillOnlyFinancialError(RuntimeError):
    """Execution positions and settlement evidence cannot be reconciled."""


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FillOnlyFinancialError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _utc(value, field="artifact timestamp").isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_value(value),
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_sha256(value: object) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _decimal_token_id(asset_id: str) -> str:
    text = str(asset_id).strip().lower()
    text = text.removeprefix("0x")
    if text.isdigit():
        return text
    if len(text) == 64 and all(char in "0123456789abcdef" for char in text):
        return str(int(text, 16))
    raise FillOnlyFinancialError("asset_id must be decimal or a 32-byte hex token id")


@dataclass(frozen=True)
class FillOnlyExecutionPosition:
    execution_run_id: str
    profile: str
    position_id: str
    market_id: int
    condition_id: str
    asset_id: str
    outcome: str
    filled_size: Decimal
    entry_notional: Decimal
    fee_paid: Decimal
    first_fill_ts: datetime
    last_fill_ts: datetime
    first_fill_block: int | None = None
    last_fill_block: int | None = None
    source_order_ids: tuple[str, ...] = ()
    source_fill_ids: tuple[str, ...] = ()
    execution_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.execution_run_id or not self.profile or not self.position_id:
            raise ValueError("execution identity fields must be non-empty")
        if self.market_id <= 0 or self.filled_size <= 0 or self.entry_notional <= 0:
            raise ValueError("positive positions require market, size, and entry notional")
        if self.fee_paid < 0:
            raise ValueError("fee_paid cannot be negative")
        if self.outcome.upper() not in {"YES", "NO"}:
            raise ValueError("outcome must be YES or NO")
        if _utc(self.first_fill_ts, field="first_fill_ts") > _utc(
            self.last_fill_ts, field="last_fill_ts"
        ):
            raise ValueError("first fill cannot follow last fill")
        if (
            self.first_fill_block is not None
            and self.last_fill_block is not None
            and self.first_fill_block > self.last_fill_block
        ):
            raise ValueError("first fill block cannot follow last fill block")
        _decimal_token_id(self.asset_id)

    @property
    def token_id(self) -> str:
        return _decimal_token_id(self.asset_id)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": POSITION_SCHEMA_VERSION,
            "execution_run_id": self.execution_run_id,
            "profile": self.profile,
            "position_id": self.position_id,
            "market_id": self.market_id,
            "condition_id": self.condition_id.lower(),
            "asset_id": self.asset_id,
            "token_id": self.token_id,
            "outcome": self.outcome.upper(),
            "filled_size": self.filled_size,
            "entry_notional": self.entry_notional,
            "fee_paid": self.fee_paid,
            "first_fill_ts": _utc(self.first_fill_ts, field="first_fill_ts"),
            "last_fill_ts": _utc(self.last_fill_ts, field="last_fill_ts"),
            "first_fill_block": self.first_fill_block,
            "last_fill_block": self.last_fill_block,
            "source_order_ids": self.source_order_ids,
            "source_fill_ids": self.source_fill_ids,
            "execution_evidence_sha256": self.execution_evidence_sha256,
        }


def write_fill_only_execution_positions(
    positions: Iterable[FillOnlyExecutionPosition],
    *,
    output_dir: Path,
    execution_source: Mapping[str, object],
) -> dict[str, object]:
    """Freeze a strategy-independent positive-position inventory."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = output_dir.resolve()
    if output.exists():
        raise FileExistsError(f"refusing to reuse execution-position output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.", dir=str(output.parent))
    )
    path = staging / "execution_positions.parquet"
    schema = pa.schema(
        [
            ("schema_version", pa.string()),
            ("execution_run_id", pa.string()),
            ("profile", pa.string()),
            ("position_id", pa.string()),
            ("market_id", pa.int64()),
            ("condition_id", pa.string()),
            ("asset_id", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("filled_size", pa.string()),
            ("entry_notional", pa.string()),
            ("fee_paid", pa.string()),
            ("first_fill_ts", pa.string()),
            ("last_fill_ts", pa.string()),
            ("first_fill_block", pa.int64()),
            ("last_fill_block", pa.int64()),
            ("execution_evidence_sha256", pa.string()),
            ("source_order_ids_json", pa.string()),
            ("source_fill_ids_json", pa.string()),
            ("position_record_sha256", pa.string()),
        ]
    )
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, object]] = []
    count = 0
    market_ids: set[int] = set()
    position_ids: set[str] = set()
    position_hash_chain = "0" * 64

    def flush() -> None:
        nonlocal writer
        if not batch:
            return
        table = pa.Table.from_pylist(batch, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(
                path, table.schema, compression="zstd", use_dictionary=True
            )
        writer.write_table(table, row_group_size=len(batch))
        batch.clear()

    try:
        try:
            for position in positions:
                if position.position_id in position_ids:
                    raise FillOnlyFinancialError(
                        f"duplicate execution position_id: {position.position_id}"
                    )
                position_ids.add(position.position_id)
                row = dict(_canonical_value(position.as_dict()))
                row["source_order_ids_json"] = _canonical_json(
                    row.pop("source_order_ids")
                )
                row["source_fill_ids_json"] = _canonical_json(
                    row.pop("source_fill_ids")
                )
                row_hash = _canonical_sha256(row)
                row["position_record_sha256"] = row_hash
                position_hash_chain = sha256(
                    bytes.fromhex(position_hash_chain) + bytes.fromhex(row_hash)
                ).hexdigest()
                market_ids.add(position.market_id)
                count += 1
                batch.append(row)
                if len(batch) >= 10_000:
                    flush()
            flush()
        finally:
            if writer is not None:
                writer.close()
        if count == 0:
            pq.write_table(
                pa.Table.from_pylist([], schema=schema), path, compression="zstd"
            )
        parquet_sha = file_sha256(path)
        manifest = {
            "schema_version": "fill_only_execution_position_manifest_v1",
            "position_count": count,
            "market_count": len(market_ids),
            "market_ids_sha256": _canonical_sha256(sorted(market_ids)),
            "position_hash_chain_sha256": position_hash_chain,
            "execution_source": dict(execution_source),
            "parquet_file": path.name,
            "parquet_file_sha256": parquet_sha,
        }
        manifest["execution_manifest_sha256"] = _canonical_sha256(manifest)
        (staging / "manifest.json").write_text(
            json.dumps(_canonical_value(manifest), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        staging.replace(output)
        return _canonical_value(manifest)  # type: ignore[return-value]
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _load_execution_position_manifest(path: Path) -> tuple[dict[str, object], Path]:
    """Verify the immutable execution-position artifact envelope."""

    root = path.resolve()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest_without_hash = {
        key: value
        for key, value in manifest.items()
        if key != "execution_manifest_sha256"
    }
    if _canonical_sha256(manifest_without_hash) != manifest.get(
        "execution_manifest_sha256"
    ):
        raise FillOnlyFinancialError("execution-position manifest hash drifted")
    parquet_path = root / str(manifest["parquet_file"])
    if file_sha256(parquet_path) != manifest["parquet_file_sha256"]:
        raise FillOnlyFinancialError("execution-position Parquet hash drifted")
    return manifest, parquet_path


def _execution_position_from_storage_row(
    raw: Mapping[str, object],
) -> tuple[FillOnlyExecutionPosition, str]:
    row = dict(raw)
    stored_hash = str(row.pop("position_record_sha256"))
    if _canonical_sha256(row) != stored_hash:
        raise FillOnlyFinancialError("execution-position record hash drifted")
    return (
        FillOnlyExecutionPosition(
            execution_run_id=str(row["execution_run_id"]),
            profile=str(row["profile"]),
            position_id=str(row["position_id"]),
            market_id=int(row["market_id"]),
            condition_id=str(row["condition_id"]),
            asset_id=str(row["asset_id"]),
            outcome=str(row["outcome"]),
            filled_size=Decimal(str(row["filled_size"])),
            entry_notional=Decimal(str(row["entry_notional"])),
            fee_paid=Decimal(str(row["fee_paid"])),
            first_fill_ts=datetime.fromisoformat(str(row["first_fill_ts"])),
            last_fill_ts=datetime.fromisoformat(str(row["last_fill_ts"])),
            first_fill_block=(
                None
                if row.get("first_fill_block") is None
                else int(row["first_fill_block"])
            ),
            last_fill_block=(
                None
                if row.get("last_fill_block") is None
                else int(row["last_fill_block"])
            ),
            source_order_ids=tuple(json.loads(str(row["source_order_ids_json"]))),
            source_fill_ids=tuple(json.loads(str(row["source_fill_ids_json"]))),
            execution_evidence_sha256=(
                None
                if row.get("execution_evidence_sha256") is None
                else str(row["execution_evidence_sha256"])
            ),
        ),
        stored_hash,
    )


def iter_fill_only_execution_positions(
    path: Path, *, batch_size: int = 10_000
) -> Iterator[FillOnlyExecutionPosition]:
    """Yield verified positions in bounded batches.

    Callers must exhaust the iterator so the terminal count and hash-chain
    checks run. The Parquet envelope is verified before the first row is
    yielded.
    """

    import pyarrow.parquet as pq

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    manifest, parquet_path = _load_execution_position_manifest(path)
    hash_chain = "0" * 64
    count = 0
    parquet = pq.ParquetFile(parquet_path)
    for batch in parquet.iter_batches(batch_size=batch_size):
        for raw in batch.to_pylist():
            position, stored_hash = _execution_position_from_storage_row(raw)
            hash_chain = sha256(
                bytes.fromhex(hash_chain) + bytes.fromhex(stored_hash)
            ).hexdigest()
            count += 1
            yield position
    if count != int(manifest["position_count"]):
        raise FillOnlyFinancialError("execution-position count drifted")
    if hash_chain != manifest["position_hash_chain_sha256"]:
        raise FillOnlyFinancialError("execution-position hash chain drifted")


def load_fill_only_execution_position_market_ids(
    path: Path,
) -> tuple[dict[str, object], tuple[int, ...]]:
    """Read only the market inventory from a verified position artifact."""

    import pyarrow.parquet as pq

    manifest, parquet_path = _load_execution_position_manifest(path)
    parquet = pq.ParquetFile(parquet_path)
    if parquet.metadata.num_rows != int(manifest["position_count"]):
        raise FillOnlyFinancialError("execution-position count drifted")
    market_ids: set[int] = set()
    for batch in parquet.iter_batches(batch_size=100_000, columns=["market_id"]):
        market_ids.update(int(value) for value in batch.column(0).to_pylist())
    ordered = tuple(sorted(market_ids))
    if len(ordered) != int(manifest["market_count"]):
        raise FillOnlyFinancialError("execution-position market count drifted")
    if _canonical_sha256(ordered) != manifest["market_ids_sha256"]:
        raise FillOnlyFinancialError("execution-position market inventory drifted")
    return manifest, ordered


def load_fill_only_execution_positions(
    path: Path,
) -> tuple[dict[str, object], tuple[FillOnlyExecutionPosition, ...]]:
    manifest, _ = _load_execution_position_manifest(path)
    return manifest, tuple(iter_fill_only_execution_positions(path))


def _position_result(
    position: FillOnlyExecutionPosition,
    settlement: MarketSettlementRecord | None,
    *,
    required_evidence_grade: str | None,
) -> dict[str, object]:
    classification = TECHNICAL_MISSING if settlement is None else settlement.classification
    reason = "MARKET_ABSENT_FROM_SETTLEMENT_CATALOG" if settlement is None else settlement.classification_reason
    payout_per_share: Decimal | None = None
    payout: Decimal | None = None
    financial_classification = classification
    finalized_at = None if settlement is None else settlement.protocol_finalized_at
    actual_evidence_grade = (
        None
        if settlement is None
        else settlement.evidence.get("evidence_grade")
    )

    if settlement is not None and settlement.classification in PAYABLE_CLASSIFICATIONS:
        accepted_evidence_grades = {
            FORMAL_EXACT_HEADER: {EXACT_CANONICAL_BLOCK_HEADER_GRADE},
            NORMAL_ORACLE_FINALIZED: {
                EXACT_CANONICAL_BLOCK_HEADER_GRADE,
                ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
            },
            RESEARCH_CUTOFF_BLOCK: {
                EXACT_CANONICAL_BLOCK_HEADER_GRADE,
                ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
                ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE,
            },
            None: {
                EXACT_CANONICAL_BLOCK_HEADER_GRADE,
                ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
                ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE,
                None,
            },
        }[required_evidence_grade]
        if actual_evidence_grade not in accepted_evidence_grades:
            financial_classification = TECHNICAL_MISSING
            reason = (
                "SETTLEMENT_EVIDENCE_GRADE_BELOW_REQUIRED:"
                f"{actual_evidence_grade or 'MISSING'}"
            )
        elif settlement.condition_id != position.condition_id.lower():
            financial_classification = IDENTITY_CONFLICT
            reason = "POSITION_CONDITION_DIFFERS_FROM_SETTLEMENT_CATALOG"
        elif settlement.payout_by_token is None or position.token_id not in settlement.payout_by_token:
            financial_classification = IDENTITY_CONFLICT
            reason = "POSITION_TOKEN_ABSENT_FROM_PAYOUT_VECTOR"
        elif required_evidence_grade in {
            NORMAL_ORACLE_FINALIZED,
            FORMAL_EXACT_HEADER,
        } and (
            position.last_fill_block is None
            or settlement.protocol_finalized_block is None
        ):
            financial_classification = TECHNICAL_MISSING
            reason = "FILL_TO_SETTLEMENT_BLOCK_ORDERING_EVIDENCE_MISSING"
        elif (
            position.last_fill_block is not None
            and settlement.protocol_finalized_block is not None
            and position.last_fill_block >= settlement.protocol_finalized_block
        ):
            financial_classification = TECHNICAL_MISSING
            reason = "FILL_BLOCK_AT_OR_AFTER_PROTOCOL_FINALIZATION_BLOCK"
        elif (
            (
                position.last_fill_block is None
                or settlement.protocol_finalized_block is None
            )
            and finalized_at is not None
            and _utc(position.last_fill_ts, field="last_fill_ts") >= finalized_at
        ):
            financial_classification = TECHNICAL_MISSING
            reason = "FILL_AT_OR_AFTER_PROTOCOL_FINALIZATION"
        elif (
            finalized_at is None
            and (
                position.last_fill_block is None
                or settlement.protocol_finalized_block is None
            )
        ):
            financial_classification = TECHNICAL_MISSING
            reason = "FILL_TO_SETTLEMENT_ORDERING_EVIDENCE_MISSING"
        else:
            payout_per_share = settlement.payout_by_token[position.token_id]
            payout = position.filled_size * payout_per_share

    settled = payout is not None
    gross_pnl = None if payout is None else payout - position.entry_notional
    net_pnl = None if payout is None else gross_pnl - position.fee_paid
    lower_payout = payout if payout is not None else Decimal(0)
    upper_payout = payout if payout is not None else position.filled_size
    result = {
        "schema_version": SETTLED_POSITION_SCHEMA_VERSION,
        **position.as_dict(),
        "settlement_catalog_record_sha256": (
            None if settlement is None else settlement.record_sha256
        ),
        "settlement_classification": financial_classification,
        "settlement_reason": reason,
        "settlement_complete": settled,
        "required_settlement_evidence": required_evidence_grade,
        "actual_settlement_evidence_grade": actual_evidence_grade,
        "protocol_finalized_at": finalized_at,
        "protocol_finalized_block": (
            None if settlement is None else settlement.protocol_finalized_block
        ),
        "payout_per_share": payout_per_share,
        "settlement_payout": payout,
        "gross_pnl_before_fee": gross_pnl,
        "net_pnl_after_recorded_fee": net_pnl,
        "payout_lower_bound": lower_payout,
        "payout_upper_bound": upper_payout,
    }
    result["financial_position_sha256"] = _canonical_sha256(result)
    return result


def _bucket(classification: str) -> str:
    if classification in PAYABLE_CLASSIFICATIONS:
        return "verified_settled"
    if classification == UNRESOLVED:
        return "unresolved_as_of_cutoff"
    if classification == POST_CUTOFF:
        return "post_cutoff"
    if classification == IDENTITY_CONFLICT:
        return "identity_conflict"
    return "technical_missing"


def _sample_sharpe(
    values: Sequence[float], *, annualization: float = 1.0
) -> Decimal | None:
    if len(values) < 2:
        return None
    deviation = statistics.stdev(values)
    if deviation == 0:
        return None
    value = statistics.mean(values) / deviation * math.sqrt(annualization)
    return None if not math.isfinite(value) else Decimal(str(value))


def _risk_metrics(
    results: Sequence[Mapping[str, object]],
    *,
    portfolio_entry_notional: Decimal,
) -> dict[str, object] | None:
    if not results or portfolio_entry_notional <= 0:
        return None
    if any(
        item.get("net_pnl_after_recorded_fee") is None
        or item.get("protocol_finalized_at") is None
        for item in results
    ):
        return None

    positive = Decimal(0)
    negative = Decimal(0)
    profitable = 0
    market_entry: dict[int, Decimal] = defaultdict(lambda: Decimal(0))
    market_pnl: dict[int, Decimal] = defaultdict(lambda: Decimal(0))
    daily_pnl: dict[datetime, Decimal] = defaultdict(lambda: Decimal(0))
    capital_days = Decimal(0)
    first_fill_day: datetime | None = None
    last_settlement_day: datetime | None = None

    for item in results:
        pnl = Decimal(str(item["net_pnl_after_recorded_fee"]))
        entry = Decimal(str(item["entry_notional"]))
        market_id = int(str(item["market_id"]))
        finalized_at = _utc(
            item["protocol_finalized_at"], field="protocol_finalized_at"
        )
        first_fill = _utc(item["first_fill_ts"], field="first_fill_ts")
        settlement_day = datetime.combine(
            finalized_at.date(), datetime.min.time(), tzinfo=UTC
        )
        fill_day = datetime.combine(first_fill.date(), datetime.min.time(), tzinfo=UTC)
        daily_pnl[settlement_day] += pnl
        market_entry[market_id] += entry
        market_pnl[market_id] += pnl
        capital_days += entry * Decimal(
            str((finalized_at - first_fill).total_seconds() / 86_400)
        )
        positive += max(pnl, Decimal(0))
        negative += min(pnl, Decimal(0))
        profitable += int(pnl > 0)
        first_fill_day = fill_day if first_fill_day is None else min(first_fill_day, fill_day)
        last_settlement_day = (
            settlement_day
            if last_settlement_day is None
            else max(last_settlement_day, settlement_day)
        )

    market_returns = [
        float(market_pnl[market_id] / entry)
        for market_id, entry in sorted(market_entry.items())
        if entry > 0
    ]
    daily_returns: list[float] = []
    assert first_fill_day is not None
    assert last_settlement_day is not None
    day = first_fill_day
    equity = portfolio_entry_notional
    peak = equity
    maximum_drawdown = Decimal(0)
    while day <= last_settlement_day:
        pnl = daily_pnl.get(day, Decimal(0))
        daily_returns.append(float(pnl / portfolio_entry_notional))
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = max(maximum_drawdown, (peak - equity) / peak)
        day += timedelta(days=1)
    downside = [min(value, 0.0) for value in daily_returns]
    downside_deviation = (
        math.sqrt(sum(value * value for value in downside) / len(downside))
        if downside and any(value < 0 for value in downside)
        else 0.0
    )
    daily_sortino = (
        statistics.mean(daily_returns) / downside_deviation * math.sqrt(365)
        if daily_returns and downside_deviation > 0
        else None
    )
    return {
        "profitable_position_fraction": Decimal(profitable) / Decimal(len(results)),
        "profit_factor": None if negative == 0 else positive / abs(negative),
        "capital_days": capital_days,
        "net_pnl_per_capital_day": (
            None
            if capital_days == 0
            else sum(
                (Decimal(str(item["net_pnl_after_recorded_fee"])) for item in results),
                Decimal(0),
            )
            / capital_days
        ),
        "maximum_drawdown_fraction": maximum_drawdown,
        "cross_market_sharpe": _sample_sharpe(market_returns),
        "annualized_daily_sharpe": _sample_sharpe(
            daily_returns, annualization=365
        ),
        "annualized_daily_sortino": (
            None
            if daily_sortino is None or not math.isfinite(daily_sortino)
            else Decimal(str(daily_sortino))
        ),
        "realized_return_days": len(daily_returns),
        "valuation_policy": "OPEN_POSITIONS_CARRIED_AT_ENTRY_COST_UNTIL_SETTLEMENT",
        "limitations": (
            "Sharpe and Sortino are descriptive realized-settlement statistics.",
            "No pre-settlement mark-to-market is inferred from OrderFilled data.",
        ),
    }


def finalize_fill_only_positions(
    positions: Iterable[FillOnlyExecutionPosition],
    settlements: Mapping[int, MarketSettlementRecord],
    *,
    execution_manifest_sha256: str,
    settlement_catalog_sha256: str,
    cutoff_ts: datetime,
    required_evidence_grade: str | None = None,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    """Finalize any strategy's Fill-only positions against one frozen catalog."""

    if required_evidence_grade not in FINANCIAL_EVIDENCE_REQUIREMENTS | {None}:
        raise FillOnlyFinancialError(
            "required_evidence_grade must be oracle-finalized, exact-header, "
            "oracle-event-cutoff-block, or None"
        )

    ordered = tuple(
        sorted(
            positions,
            key=lambda item: (
                item.last_fill_ts,
                item.market_id,
                item.token_id,
                item.position_id,
            ),
        )
    )
    if len({item.position_id for item in ordered}) != len(ordered):
        raise FillOnlyFinancialError("position ids must be unique")
    results = tuple(
        _position_result(
            position,
            settlements.get(position.market_id),
            required_evidence_grade=required_evidence_grade,
        )
        for position in ordered
    )
    counts: Counter[str] = Counter(
        _bucket(str(item["settlement_classification"])) for item in results
    )
    total_entry = sum((item.entry_notional for item in ordered), Decimal(0))
    total_fees = sum((item.fee_paid for item in ordered), Decimal(0))
    known_payout = sum(
        (Decimal(str(item["settlement_payout"])) for item in results if item["settlement_payout"] is not None),
        Decimal(0),
    )
    payout_lower = sum((Decimal(str(item["payout_lower_bound"])) for item in results), Decimal(0))
    payout_upper = sum((Decimal(str(item["payout_upper_bound"])) for item in results), Decimal(0))
    complete = bool(results) and counts["verified_settled"] == len(results)
    status = (
        "N/A_NO_DEPLOYED_CAPITAL"
        if not results
        else "FINANCIAL_COMPLETE"
        if complete
        else "FINANCIAL_PARTIAL"
    )
    settled_results = tuple(item for item in results if item["settlement_complete"])
    settled_entry = sum(
        (Decimal(str(item["entry_notional"])) for item in settled_results), Decimal(0)
    )
    settled_fees = sum(
        (Decimal(str(item["fee_paid"])) for item in settled_results), Decimal(0)
    )
    settled_pnl = known_payout - settled_entry - settled_fees
    complete_risk_metrics = (
        _risk_metrics(results, portfolio_entry_notional=total_entry)
        if complete
        else None
    )
    settled_subset_risk_metrics = _risk_metrics(
        settled_results, portfolio_entry_notional=settled_entry
    )
    fee_scenarios = tuple(
        {
            "fee_bps_on_entry_notional": fee_bps,
            "assumed_fee": total_entry * Decimal(fee_bps) / Decimal(10_000),
            "complete_portfolio_net_pnl": (
                None
                if not complete
                else known_payout
                - total_entry
                - total_entry * Decimal(fee_bps) / Decimal(10_000)
            ),
        }
        for fee_bps in FEE_SCENARIO_BPS
    )
    summary = {
        "schema_version": FINANCIAL_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "cutoff_ts": _utc(cutoff_ts, field="cutoff_ts"),
        "execution_manifest_sha256": execution_manifest_sha256,
        "settlement_catalog_sha256": settlement_catalog_sha256,
        "required_settlement_evidence": required_evidence_grade,
        "position_count": len(results),
        "market_count": len({item.market_id for item in ordered}),
        "classification_counts": dict(sorted(counts.items())),
        "full_portfolio": {
            "entry_notional": total_entry,
            "recorded_fee": total_fees,
            "settlement_payout": known_payout if complete else None,
            "net_pnl": known_payout - total_entry - total_fees if complete else None,
            "net_roi": (
                (known_payout - total_entry - total_fees) / total_entry
                if complete and total_entry > 0
                else None
            ),
            "risk_metrics": complete_risk_metrics,
            "risk_metrics_reason": (
                None
                if complete_risk_metrics is not None
                else (
                    "SETTLEMENT_TIMESTAMPS_INCOMPLETE"
                    if complete
                    else "INCOMPLETE_PORTFOLIO_SETTLEMENT"
                )
            ),
        },
        "verified_settled_subset_diagnostic": {
            "warning": "SELECTION_BIASED_DIAGNOSTIC_NOT_FULL_PORTFOLIO_RETURN",
            "position_count": len(settled_results),
            "entry_notional": settled_entry,
            "settlement_payout": known_payout,
            "recorded_fee": settled_fees,
            "net_pnl": settled_pnl,
            "net_roi": settled_pnl / settled_entry if settled_entry > 0 else None,
            "risk_metrics": settled_subset_risk_metrics,
            "risk_metrics_reason": (
                None
                if settled_subset_risk_metrics is not None
                else "SETTLEMENT_TIMESTAMPS_INCOMPLETE_OR_SAMPLE_TOO_SMALL"
            ),
        },
        "unresolved_and_technical_bounds": {
            "known_settlement_payout": known_payout,
            "portfolio_payout_lower_bound": payout_lower,
            "portfolio_payout_upper_bound": payout_upper,
            "portfolio_net_pnl_lower_bound": payout_lower - total_entry - total_fees,
            "portfolio_net_pnl_upper_bound": payout_upper - total_entry - total_fees,
        },
        "fee_scenarios": fee_scenarios,
    }
    summary["summary_sha256"] = _canonical_sha256(summary)
    return summary, results


def write_fill_only_financial_bundle(
    *,
    output_dir: Path,
    summary: Mapping[str, object],
    positions: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Persist financial results without mutating the execution artifacts."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = output_dir.resolve()
    if output.exists():
        raise FileExistsError(f"refusing to reuse financial output: {output}")
    output.mkdir(parents=True)

    position_schema = pa.schema(
        [
            ("schema_version", pa.string()),
            ("execution_run_id", pa.string()),
            ("profile", pa.string()),
            ("position_id", pa.string()),
            ("market_id", pa.int64()),
            ("condition_id", pa.string()),
            ("asset_id", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("filled_size", pa.string()),
            ("entry_notional", pa.string()),
            ("fee_paid", pa.string()),
            ("first_fill_ts", pa.string()),
            ("last_fill_ts", pa.string()),
            ("first_fill_block", pa.int64()),
            ("last_fill_block", pa.int64()),
            ("execution_evidence_sha256", pa.string()),
            ("settlement_catalog_record_sha256", pa.string()),
            ("settlement_classification", pa.string()),
            ("settlement_reason", pa.string()),
            ("settlement_complete", pa.bool_()),
            ("required_settlement_evidence", pa.string()),
            ("actual_settlement_evidence_grade", pa.string()),
            ("protocol_finalized_at", pa.string()),
            ("protocol_finalized_block", pa.int64()),
            ("payout_per_share", pa.string()),
            ("settlement_payout", pa.string()),
            ("gross_pnl_before_fee", pa.string()),
            ("net_pnl_after_recorded_fee", pa.string()),
            ("payout_lower_bound", pa.string()),
            ("payout_upper_bound", pa.string()),
            ("financial_position_sha256", pa.string()),
            ("source_order_ids_json", pa.string()),
            ("source_fill_ids_json", pa.string()),
        ]
    )
    cashflow_schema = pa.schema(
        [
            ("position_id", pa.string()),
            ("market_id", pa.int64()),
            ("cashflow_type", pa.string()),
            ("cashflow_ts", pa.string()),
            ("amount", pa.string()),
            ("settlement_classification", pa.string()),
        ]
    )
    position_path = output / "position_ledger.parquet"
    cashflow_path = output / "cashflow_ledger.parquet"
    daily_path = output / "daily_realized_equity.parquet"
    position_writer = pq.ParquetWriter(
        position_path, position_schema, compression="zstd", use_dictionary=True
    )
    cashflow_writer = pq.ParquetWriter(
        cashflow_path, cashflow_schema, compression="zstd", use_dictionary=True
    )
    position_rows: list[dict[str, object]] = []
    cashflow_rows: list[dict[str, object]] = []
    position_count = 0
    cashflow_count = 0

    def flush_positions() -> None:
        if not position_rows:
            return
        position_writer.write_table(
            pa.Table.from_pylist(position_rows, schema=position_schema),
            row_group_size=len(position_rows),
        )
        position_rows.clear()

    def flush_cashflows() -> None:
        if not cashflow_rows:
            return
        cashflow_writer.write_table(
            pa.Table.from_pylist(cashflow_rows, schema=cashflow_schema),
            row_group_size=len(cashflow_rows),
        )
        cashflow_rows.clear()

    daily_realized: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "settlement_payout": Decimal(0),
            "released_entry_notional": Decimal(0),
            "released_recorded_fee": Decimal(0),
            "realized_net_pnl": Decimal(0),
        }
    )
    try:
        for item in positions:
            row = dict(_canonical_value(item))
            row["source_order_ids_json"] = _canonical_json(
                row.pop("source_order_ids")
            )
            row["source_fill_ids_json"] = _canonical_json(
                row.pop("source_fill_ids")
            )
            position_rows.append(row)
            position_count += 1
            entry_notional = Decimal(str(item["entry_notional"]))
            recorded_fee = Decimal(str(item["fee_paid"]))
            first_fill_at = str(_canonical_value(item["first_fill_ts"]))
            cashflow_rows.append(
                {
                    "position_id": item["position_id"],
                    "market_id": item["market_id"],
                    "cashflow_type": "ENTRY_NOTIONAL",
                    "cashflow_ts": first_fill_at,
                    "amount": str(-entry_notional),
                    "settlement_classification": item["settlement_classification"],
                }
            )
            cashflow_count += 1
            if recorded_fee != 0:
                cashflow_rows.append(
                    {
                        "position_id": item["position_id"],
                        "market_id": item["market_id"],
                        "cashflow_type": "RECORDED_FEE",
                        "cashflow_ts": first_fill_at,
                        "amount": str(-recorded_fee),
                        "settlement_classification": item[
                            "settlement_classification"
                        ],
                    }
                )
                cashflow_count += 1
            if item.get("settlement_payout") is not None:
                finalized_at = str(_canonical_value(item["protocol_finalized_at"]))
                payout = Decimal(str(item["settlement_payout"]))
                net_pnl = Decimal(str(item["net_pnl_after_recorded_fee"]))
                cashflow_rows.append(
                    {
                        "position_id": item["position_id"],
                        "market_id": item["market_id"],
                        "cashflow_type": "SETTLEMENT_PAYOUT",
                        "cashflow_ts": finalized_at,
                        "amount": str(payout),
                        "settlement_classification": item[
                            "settlement_classification"
                        ],
                    }
                )
                cashflow_count += 1
                daily_realized[finalized_at[:10]]["settlement_payout"] += payout
                daily_realized[finalized_at[:10]][
                    "released_entry_notional"
                ] += entry_notional
                daily_realized[finalized_at[:10]][
                    "released_recorded_fee"
                ] += recorded_fee
                daily_realized[finalized_at[:10]]["realized_net_pnl"] += net_pnl
            if len(position_rows) >= 10_000:
                flush_positions()
            if len(cashflow_rows) >= 10_000:
                flush_cashflows()
        flush_positions()
        flush_cashflows()
    finally:
        position_writer.close()
        cashflow_writer.close()
    cumulative_pnl = Decimal(0)
    initial_equity = Decimal(
        str(dict(summary.get("full_portfolio") or {}).get("entry_notional") or 0)
    )
    daily_rows: list[dict[str, object]] = []
    for day, values in sorted(daily_realized.items()):
        cumulative_pnl += values["realized_net_pnl"]
        daily_rows.append(
            {
                "date": day,
                **{key: str(value) for key, value in values.items()},
                "cumulative_net_pnl": str(cumulative_pnl),
                "realized_equity_index": str(initial_equity + cumulative_pnl),
                "valuation_policy": (
                    "OPEN_POSITIONS_CARRIED_AT_ENTRY_COST_UNTIL_SETTLEMENT"
                ),
            }
        )
    pq.write_table(pa.Table.from_pylist(daily_rows), daily_path, compression="zstd")
    summary_path = output / "financial_summary.json"
    summary_path.write_text(
        json.dumps(_canonical_value(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    file_hashes = {
        path.name: file_sha256(path)
        for path in (position_path, cashflow_path, daily_path, summary_path)
    }
    manifest = {
        "schema_version": "fill_only_financial_bundle_manifest_v1",
        "execution_manifest_sha256": summary["execution_manifest_sha256"],
        "settlement_catalog_sha256": summary["settlement_catalog_sha256"],
        "financial_summary_sha256": summary["summary_sha256"],
        "position_count": position_count,
        "cashflow_count": cashflow_count,
        "files": file_hashes,
    }
    manifest["bundle_sha256"] = _canonical_sha256(manifest)
    (output / "manifest.json").write_text(
        json.dumps(_canonical_value(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return _canonical_value(manifest)  # type: ignore[return-value]
