"""Validation helpers for fill-first performance score rankings."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_performance_score_validation_report(
    rows: Sequence[Any],
    *,
    min_reliable_closed_trades: int = 3,
    top_n: int = 5,
    max_small_sample_top_share_pct: Decimal | int | str = Decimal("40"),
) -> dict[str, Any]:
    """Check whether performance-score rankings are dominated by tiny samples."""

    scored = [_normalize_row(row, index=index) for index, row in enumerate(rows or [], start=1)]
    scored = [row for row in scored if row["has_score"]]
    if not scored:
        return {
            "schema_version": "fill_first_performance_score_validation_v1",
            "status": MISSING,
            "score_bias_verdict": MISSING,
            "reason": "no performance_score rows supplied",
            "rows_scored": 0,
            "reliable_row_count": 0,
            "small_sample_row_count": 0,
            "top_n": int(top_n),
            "top_small_sample_count": 0,
            "top_small_sample_share_pct": "0",
            "small_sample_score_premium": "0",
            "best_small_sample": {},
            "best_reliable_sample": {},
            "top_rows": [],
            "next_actions": ["Include performance_score and closed_trade_count/sample_count in parameter search result rows."],
        }

    ordered = sorted(scored, key=lambda row: (row["score"], row["sample_count"], row["net_pnl"]), reverse=True)
    top_rows = ordered[: max(1, min(int(top_n), len(ordered)))]
    small_sample_rows = [row for row in ordered if row["sample_count"] < int(min_reliable_closed_trades)]
    reliable_rows = [row for row in ordered if row["sample_count"] >= int(min_reliable_closed_trades)]
    top_small = [row for row in top_rows if row["sample_count"] < int(min_reliable_closed_trades)]
    top_small_share_pct = _pct(len(top_small), len(top_rows))
    max_share = _decimal(max_small_sample_top_share_pct)
    best_small = small_sample_rows[0] if small_sample_rows else {}
    best_reliable = reliable_rows[0] if reliable_rows else {}
    small_sample_score_premium = (
        _decimal(best_small.get("score")) - _decimal(best_reliable.get("score"))
        if best_small and best_reliable
        else Decimal("0")
    )

    review_reasons: list[str] = []
    if len(scored) < 3:
        review_reasons.append(f"only {len(scored)} scored rows; need at least 3")
    if not reliable_rows:
        review_reasons.append(f"no rows with at least {min_reliable_closed_trades} closed trades")
    if top_small_share_pct > max_share:
        review_reasons.append(f"small-sample rows are {top_small_share_pct}% of top {len(top_rows)}; max {max_share}%")
    if small_sample_score_premium > Decimal("0"):
        review_reasons.append(f"best small-sample score exceeds best reliable score by {_decimal_text(small_sample_score_premium)}")

    verdict = REVIEW if review_reasons else READY
    return {
        "schema_version": "fill_first_performance_score_validation_v1",
        "status": READY,
        "score_bias_verdict": verdict,
        "reason": "performance score ranking is not dominated by small samples" if verdict == READY else "; ".join(review_reasons),
        "rows_scored": len(scored),
        "reliable_row_count": len(reliable_rows),
        "small_sample_row_count": len(small_sample_rows),
        "min_reliable_closed_trades": int(min_reliable_closed_trades),
        "top_n": len(top_rows),
        "top_small_sample_count": len(top_small),
        "top_small_sample_share_pct": _decimal_text(top_small_share_pct),
        "small_sample_score_premium": _decimal_text(small_sample_score_premium),
        "best_small_sample": _public_row(best_small) if best_small else {},
        "best_reliable_sample": _public_row(best_reliable) if best_reliable else {},
        "top_rows": [_public_row(row) for row in top_rows],
        "next_actions": _next_actions(verdict, review_reasons),
    }


def _normalize_row(row: Any, *, index: int) -> dict[str, Any]:
    plain = _as_mapping(_as_plain(row))
    payload = _as_mapping(plain.get("payload"))
    combined = {**payload, **plain}
    score_value = _first_present(combined, "performance_score", "performanceScore")
    sample_count = _first_present(combined, "closed_trade_count", "closedTradeCount", "sample_count", "sampleCount", "trades", "trade_count", "tradeCount")
    return {
        "row_index": index,
        "source": plain,
        "run_id": _first_present(combined, "run_id", "runId", "benchmark_id", "benchmarkId", "key") or f"row-{index}",
        "parameter_fingerprint": _first_present(combined, "parameter_fingerprint", "parameterFingerprint"),
        "evidence_mode": _first_present(combined, "evidence_mode", "evidenceMode", "mode", "phase", "sample"),
        "score": _decimal(score_value),
        "has_score": score_value not in (None, ""),
        "sample_count": int(max(Decimal("0"), _decimal(sample_count))),
        "net_pnl": _decimal(_first_present(combined, "net_pnl", "netPnl", "total_pnl", "totalPnl", "pnl", "netProfit")),
        "ranking_verdict": str(_first_present(combined, "ranking_verdict", "rankingVerdict") or ""),
    }


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": row.get("run_id"),
        "parameter_fingerprint": row.get("parameter_fingerprint") or "",
        "evidence_mode": row.get("evidence_mode") or "",
        "performance_score": _decimal_text(_decimal(row.get("score"))),
        "sample_count": int(row.get("sample_count") or 0),
        "net_pnl": _decimal_text(_decimal(row.get("net_pnl"))),
        "ranking_verdict": row.get("ranking_verdict") or "",
    }


def _next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Performance-score ranking can be used, while still displaying sample count, fill quality, and prediction quality."]
    return [f"Review performance-score ranking bias: {reason}" for reason in review_reasons[:5]]


def _pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator) * Decimal("100")


def _first_present(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in source and source.get(key) not in (None, ""):
            return source.get(key)
    return None


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "__dict__"):
        return {str(key): inner for key, inner in vars(value).items()}
    return {}


def _as_plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _as_plain(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_as_plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(key): _as_plain(inner) for key, inner in vars(value).items()}
    return value


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP).normalize()
    return format(normalized, "f")
