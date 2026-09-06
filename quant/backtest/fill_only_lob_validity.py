"""LOB-holdout calibrated validity rules for fill-only replay.

Runtime use stays fill-only: this module only inspects simulated orders and
trade prints.  LOB data is used offline to choose the saved rule parameters.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

Q = Decimal("0.0000000001")
Eligibility = str


@dataclass(frozen=True)
class FillOnlyValidityFeatures:
    side: str
    limit_price: Decimal
    price_bucket: str
    last_same_side_trade_age_seconds: Decimal | None
    last_opposite_side_trade_age_seconds: Decimal | None
    trailing_same_side_volume: Decimal
    trailing_same_side_count: int
    trailing_opposite_volume: Decimal
    trailing_opposite_count: int
    future_eligible_trade_count: int
    future_eligible_volume: Decimal
    price_buffer: Decimal
    participation_rate: Decimal
    horizon_seconds: Decimal | None
    has_pre_arrival_quote_proxy: bool

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        return {key: _json_ready(value) for key, value in row.items()}


@dataclass(frozen=True)
class FillOnlyValidityRule:
    name: str = "lob_holdout_calibrated_fill_only"
    min_probability: Decimal = Decimal("0.75")
    medium_probability: Decimal = Decimal("0.60")
    high_probability: Decimal = Decimal("0.80")
    capacity_scale: bool = True
    require_pre_arrival_quote_proxy: bool = True
    trailing_window_seconds: Decimal = Decimal("1800")
    min_trailing_same_side_count: int = 1
    min_trailing_same_side_volume: Decimal = Decimal("0")
    min_trailing_opposite_side_count: int = 0
    min_future_eligible_trade_count: int = 1
    min_future_eligible_volume: Decimal = Decimal("0")
    max_horizon_seconds: Decimal | None = None

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        return {key: _json_ready(value) for key, value in row.items()}


@dataclass(frozen=True)
class FillOnlyValidityDecision:
    p_depth_valid: Decimal
    capacity_multiplier: Decimal
    eligibility: Eligibility
    accepted: bool
    reason: str
    features: FillOnlyValidityFeatures
    rule: FillOnlyValidityRule

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        return {key: _json_ready(value) for key, value in row.items()}


class FillOnlyLobValidityModel:
    """Rule model calibrated offline from LOB holdout labels.

    The model's runtime inputs are deliberately restricted to fill-only
    features.  It does not read LOB snapshots during replay.
    """

    def __init__(
        self, rule: FillOnlyValidityRule | Mapping[str, Any] | None = None
    ) -> None:
        if rule is None:
            self.rule = default_lob_holdout_validity_rule()
        elif isinstance(rule, FillOnlyValidityRule):
            self.rule = rule
        else:
            self.rule = fill_only_validity_rule_from_mapping(rule)

    def decide(
        self,
        order: Any,
        trades: Iterable[Any],
        *,
        side: str,
        limit: Decimal,
        price_buffer: Decimal,
    ) -> FillOnlyValidityDecision:
        features = extract_fill_only_validity_features(
            order,
            trades,
            side=side,
            limit=limit,
            price_buffer=price_buffer,
            trailing_window=timedelta(seconds=float(self.rule.trailing_window_seconds)),
        )
        return evaluate_fill_only_validity(features, self.rule)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "fill_only_lob_validity_model_v1",
            "rule": self.rule.as_dict(),
        }


def default_lob_holdout_validity_rule() -> FillOnlyValidityRule:
    return FillOnlyValidityRule()


def load_lob_holdout_validity_rule(path: str | Path | None) -> FillOnlyValidityRule:
    if not path:
        return default_lob_holdout_validity_rule()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rule_payload = payload.get("rule") if isinstance(payload, Mapping) else None
    if not isinstance(rule_payload, Mapping) and isinstance(payload, Mapping):
        rule_payload = payload.get("params")
    if not isinstance(rule_payload, Mapping):
        rule_payload = payload
    return fill_only_validity_rule_from_mapping(rule_payload)


def save_lob_holdout_validity_rule(
    path: str | Path,
    rule: FillOnlyValidityRule,
    *,
    report: Mapping[str, Any] | None = None,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "schema_version": "fill_only_lob_validity_rule_v1",
        "rule": rule.as_dict(),
    }
    if report is not None:
        payload["report"] = _json_ready(dict(report))
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def fill_only_validity_rule_from_mapping(
    row: Mapping[str, Any],
) -> FillOnlyValidityRule:
    return FillOnlyValidityRule(
        name=str(row.get("name") or "lob_holdout_calibrated_fill_only"),
        min_probability=_decimal(
            row.get("min_probability", row.get("p_depth_valid_threshold", "0.75"))
        ),
        medium_probability=_decimal(row.get("medium_probability", "0.60")),
        high_probability=_decimal(row.get("high_probability", "0.80")),
        capacity_scale=bool(row.get("capacity_scale", True)),
        require_pre_arrival_quote_proxy=bool(
            row.get("require_pre_arrival_quote_proxy", True)
        ),
        trailing_window_seconds=_decimal(
            row.get(
                "trailing_window_seconds", row.get("quote_proxy_ttl_seconds", "1800")
            )
        ),
        min_trailing_same_side_count=int(
            row.get("min_trailing_same_side_count", 1) or 0
        ),
        min_trailing_same_side_volume=_decimal(
            row.get("min_trailing_same_side_volume", "0")
        ),
        min_trailing_opposite_side_count=int(
            row.get("min_trailing_opposite_side_count", 0) or 0
        ),
        min_future_eligible_trade_count=int(
            row.get("min_future_eligible_trade_count", 1) or 0
        ),
        min_future_eligible_volume=_decimal(row.get("min_future_eligible_volume", "0")),
        max_horizon_seconds=_optional_decimal(row.get("max_horizon_seconds")),
    )


def extract_fill_only_validity_features(
    order: Any,
    trades: Iterable[Any],
    *,
    side: str,
    limit: Decimal,
    price_buffer: Decimal,
    trailing_window: timedelta | None = None,
) -> FillOnlyValidityFeatures:
    side_text = str(side).upper()
    arrival_ts = getattr(order, "arrival_ts", None)
    deadline_ts = getattr(order, "deadline_ts", None)
    arrival_block = getattr(order, "arrival_block", None)
    deadline_block = getattr(order, "deadline_block", None)
    window = trailing_window or timedelta(seconds=1800)
    trailing_start = arrival_ts - window if isinstance(arrival_ts, datetime) else None
    last_same: datetime | None = None
    last_opposite: datetime | None = None
    trailing_same_count = 0
    trailing_opp_count = 0
    trailing_same_volume = Decimal("0")
    trailing_opp_volume = Decimal("0")
    future_count = 0
    future_volume = Decimal("0")
    has_quote_proxy = False

    for trade in sorted(trades, key=lambda item: getattr(item, "sequence", ())):
        if not _same_market_asset(order, trade):
            continue
        trade_side = str(getattr(trade, "aggressor_side", "")).upper()
        trade_ts = getattr(trade, "block_time", None)
        trade_price = _decimal(getattr(trade, "price", "0"))
        trade_size = _decimal(getattr(trade, "size", "0"))
        if _before_arrival(order, trade):
            if trade_side == side_text:
                last_same = _max_dt(last_same, trade_ts)
                if _inside_trailing_window(
                    trade_ts, trailing_start, arrival_ts
                ) and _limit_allows(side_text, trade_price, limit):
                    trailing_same_count += 1
                    trailing_same_volume += trade_size
                    has_quote_proxy = True
            elif trade_side in {"BUY", "SELL"}:
                last_opposite = _max_dt(last_opposite, trade_ts)
                if _inside_trailing_window(trade_ts, trailing_start, arrival_ts):
                    trailing_opp_count += 1
                    trailing_opp_volume += trade_size
        if (
            _after_arrival(order, trade)
            and _before_deadline(order, trade)
            and trade_side == side_text
            and _limit_allows(side_text, trade_price, limit)
        ):
            exec_price = _exec_price(side_text, trade_price, price_buffer)
            if _limit_allows(side_text, exec_price, limit):
                future_count += 1
                future_volume += trade_size

    return FillOnlyValidityFeatures(
        side=side_text,
        limit_price=_decimal(limit).quantize(Q, rounding=ROUND_HALF_UP),
        price_bucket=_price_bucket(limit),
        last_same_side_trade_age_seconds=_age_seconds(arrival_ts, last_same),
        last_opposite_side_trade_age_seconds=_age_seconds(arrival_ts, last_opposite),
        trailing_same_side_volume=trailing_same_volume.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        trailing_same_side_count=trailing_same_count,
        trailing_opposite_volume=trailing_opp_volume.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        trailing_opposite_count=trailing_opp_count,
        future_eligible_trade_count=future_count,
        future_eligible_volume=future_volume.quantize(Q, rounding=ROUND_HALF_UP),
        price_buffer=_decimal(price_buffer).quantize(Q, rounding=ROUND_HALF_UP),
        participation_rate=_decimal(getattr(order, "participation_rate", "0")).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        horizon_seconds=_horizon_seconds(
            order, arrival_ts, deadline_ts, arrival_block, deadline_block
        ),
        has_pre_arrival_quote_proxy=has_quote_proxy,
    )


def evaluate_fill_only_validity(
    features: FillOnlyValidityFeatures, rule: FillOnlyValidityRule
) -> FillOnlyValidityDecision:
    score = Decimal("1")
    reason = ""
    if (
        rule.require_pre_arrival_quote_proxy
        and not features.has_pre_arrival_quote_proxy
    ):
        score = Decimal("0")
        reason = "lob_holdout_calibrated_missing_quote_proxy"
    score = min(
        score,
        _ratio(features.trailing_same_side_count, rule.min_trailing_same_side_count),
    )
    score = min(
        score,
        _ratio(features.trailing_same_side_volume, rule.min_trailing_same_side_volume),
    )
    score = min(
        score,
        _ratio(features.trailing_opposite_count, rule.min_trailing_opposite_side_count),
    )
    score = min(
        score,
        _ratio(
            features.future_eligible_trade_count, rule.min_future_eligible_trade_count
        ),
    )
    score = min(
        score, _ratio(features.future_eligible_volume, rule.min_future_eligible_volume)
    )
    if (
        rule.max_horizon_seconds is not None
        and features.horizon_seconds is not None
        and features.horizon_seconds > rule.max_horizon_seconds
    ):
        score = Decimal("0")
        reason = "lob_holdout_calibrated_horizon_too_long"
    score = max(Decimal("0"), min(Decimal("1"), score)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    eligibility = fill_only_eligibility(score, rule)
    if not reason and score < _decimal(rule.min_probability):
        reason = "low_p_depth_valid"
    accepted = score >= _decimal(rule.min_probability)
    multiplier = score if rule.capacity_scale else Decimal("1")
    if not accepted:
        multiplier = Decimal("0")
    return FillOnlyValidityDecision(
        p_depth_valid=score,
        capacity_multiplier=multiplier.quantize(Q, rounding=ROUND_HALF_UP),
        eligibility=eligibility,
        accepted=accepted,
        reason=reason,
        features=features,
        rule=rule,
    )


def fill_only_eligibility(
    p_depth_valid: Decimal, rule: FillOnlyValidityRule
) -> Eligibility:
    score = _decimal(p_depth_valid)
    if score >= _decimal(rule.high_probability):
        return "ELIGIBLE_HIGH"
    if score >= _decimal(rule.medium_probability):
        return "ELIGIBLE_MEDIUM"
    if score > 0:
        return "ELIGIBLE_LOW"
    return "UNSUPPORTED"


def fill_only_validity_rule_grid() -> list[FillOnlyValidityRule]:
    rows: list[FillOnlyValidityRule] = []
    for min_probability in (Decimal("0.60"), Decimal("0.75"), Decimal("0.90")):
        for trailing_count in (1, 2):
            for future_count in (1, 2):
                for opposite_count in (0, 1):
                    for quote_ttl in (Decimal("300"), Decimal("1800")):
                        for trailing_volume in (Decimal("0"), Decimal("10")):
                            rows.append(
                                FillOnlyValidityRule(
                                    min_probability=min_probability,
                                    medium_probability=min(
                                        Decimal("0.60"), min_probability
                                    ),
                                    high_probability=max(
                                        Decimal("0.80"), min_probability
                                    ),
                                    trailing_window_seconds=quote_ttl,
                                    min_trailing_same_side_count=trailing_count,
                                    min_trailing_same_side_volume=trailing_volume,
                                    min_trailing_opposite_side_count=opposite_count,
                                    min_future_eligible_trade_count=future_count,
                                )
                            )
    return rows


def holdout_loss(metrics_or_counts: Mapping[str, Any], *, samples: int) -> Decimal:
    total = Decimal(max(1, int(samples or 0)))
    counts_raw = metrics_or_counts.get("verdict_counts")
    counts: Mapping[str, Any] = (
        counts_raw if isinstance(counts_raw, Mapping) else metrics_or_counts
    )
    false_positive = _metric_rate(
        metrics_or_counts, "false_positive_rate", counts, "fill_only_only", total
    )
    false_negative = _metric_rate(
        metrics_or_counts, "false_negative_rate", counts, "depth_only", total
    )
    overfill = _metric_rate(
        metrics_or_counts, "overfill_rate", counts, "overfill", total
    )
    underfill = _metric_rate(
        metrics_or_counts, "underfill_rate", counts, "underfill", total
    )
    adverse_price = _decimal(metrics_or_counts.get("adverse_price_error", "0"))
    return (
        Decimal("5") * false_positive
        + Decimal("2") * overfill
        + Decimal("2") * adverse_price
        + false_negative
        + Decimal("0.5") * underfill
    ).quantize(Q, rounding=ROUND_HALF_UP)


def build_execution_stability_report(
    split_metrics: Mapping[str, Mapping[str, Any]],
    *,
    min_split_samples: int = 100,
    target_total_samples: int = 1000,
) -> dict[str, Any]:
    """Score whether a calibrated fill-only profile is stable enough to trust.

    This is intentionally conservative: good holdout rates are not enough when
    the labeled sample is tiny or concentrated in a single split.
    """

    required = ("calibration", "validation", "holdout")
    rows = {name: split_metrics.get(name, {}) for name in required}
    sample_counts = {name: int(rows[name].get("samples") or 0) for name in required}
    total_samples = sum(sample_counts.values())
    quality_by_split: dict[str, Decimal] = {}
    false_positive_rates: list[Decimal] = []
    overfill_rates: list[Decimal] = []
    adverse_price_errors: list[Decimal] = []
    for name in required:
        row = rows[name]
        samples = sample_counts[name]
        if samples <= 0:
            quality_by_split[name] = Decimal("0")
        else:
            loss = holdout_loss(row, samples=samples)
            quality_by_split[name] = (Decimal("1") - min(Decimal("1"), loss)).quantize(
                Q, rounding=ROUND_HALF_UP
            )
        false_positive_rates.append(_decimal(row.get("false_positive_rate", "0")))
        overfill_rates.append(_decimal(row.get("overfill_rate", "0")))
        adverse_price_errors.append(_decimal(row.get("adverse_price_error", "0")))

    avg_quality = (
        sum(quality_by_split.values(), Decimal("0")) / Decimal(len(required))
    ).quantize(Q, rounding=ROUND_HALF_UP)
    sample_coverage = _bounded_ratio(
        Decimal(total_samples), Decimal(max(1, int(target_total_samples)))
    )
    min_split_coverage = min(
        _bounded_ratio(Decimal(count), Decimal(max(1, int(min_split_samples))))
        for count in sample_counts.values()
    )
    spread_penalty = (
        _spread(false_positive_rates) * Decimal("2")
        + _spread(overfill_rates)
        + _spread(adverse_price_errors) * Decimal("10")
    )
    spread_penalty = min(Decimal("0.30"), spread_penalty)
    score = (
        Decimal("0.55") * avg_quality
        + Decimal("0.25") * sample_coverage
        + Decimal("0.20") * min_split_coverage
        - spread_penalty
    )
    score = max(Decimal("0"), min(Decimal("1"), score)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    if (
        score >= Decimal("0.80")
        and min_split_coverage >= Decimal("1")
        and avg_quality >= Decimal("0.85")
    ):
        grade = "READY"
    elif score >= Decimal("0.50"):
        grade = "REVIEW"
    else:
        grade = "LOW_CONFIDENCE"
    blockers: list[str] = []
    if total_samples < int(target_total_samples):
        blockers.append("insufficient_total_holdout_samples")
    for name, count in sample_counts.items():
        if count < int(min_split_samples):
            blockers.append(f"insufficient_{name}_samples")
    if spread_penalty > Decimal("0.10"):
        blockers.append("unstable_split_metrics")
    return {
        "schema_version": "fill_only_execution_stability_v1",
        "score": str(score),
        "grade": grade,
        "sample_counts": sample_counts,
        "target_total_samples": int(target_total_samples),
        "min_split_samples": int(min_split_samples),
        "sample_coverage": str(sample_coverage),
        "min_split_coverage": str(min_split_coverage),
        "avg_split_quality": str(avg_quality),
        "quality_by_split": {
            key: str(value) for key, value in quality_by_split.items()
        },
        "spread_penalty": str(spread_penalty.quantize(Q, rounding=ROUND_HALF_UP)),
        "blockers": blockers,
    }


def _metric_rate(
    metrics: Mapping[str, Any],
    key: str,
    counts: Mapping[str, Any],
    count_key: str,
    total: Decimal,
) -> Decimal:
    if key in metrics:
        return _decimal(metrics.get(key))
    return Decimal(int(counts.get(count_key, 0) or 0)) / total


def _bounded_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return max(Decimal("0"), min(Decimal("1"), numerator / denominator)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _spread(values: Iterable[Decimal]) -> Decimal:
    rows = list(values)
    if not rows:
        return Decimal("0")
    return (max(rows) - min(rows)).copy_abs()


def _same_market_asset(order: Any, trade: Any) -> bool:
    return (
        int(getattr(order, "market_id")) == int(getattr(trade, "market_id"))
        and str(getattr(order, "asset_id")).lower()
        == str(getattr(trade, "asset_id")).lower()
    )


def _before_arrival(order: Any, trade: Any) -> bool:
    arrival_block = getattr(order, "arrival_block", None)
    arrival_ts = getattr(order, "arrival_ts", None)
    if arrival_block is not None and int(getattr(trade, "block_number")) >= int(
        arrival_block
    ):
        return False
    if arrival_ts is not None and getattr(trade, "block_time") >= arrival_ts:
        return False
    return True


def _after_arrival(order: Any, trade: Any) -> bool:
    arrival_block = getattr(order, "arrival_block", None)
    arrival_ts = getattr(order, "arrival_ts", None)
    if arrival_block is not None and int(getattr(trade, "block_number")) < int(
        arrival_block
    ):
        return False
    if arrival_ts is not None and getattr(trade, "block_time") < arrival_ts:
        return False
    return True


def _before_deadline(order: Any, trade: Any) -> bool:
    deadline_block = getattr(order, "deadline_block", None)
    deadline_ts = getattr(order, "deadline_ts", None)
    if deadline_block is not None and int(getattr(trade, "block_number")) > int(
        deadline_block
    ):
        return False
    if deadline_ts is not None and getattr(trade, "block_time") > deadline_ts:
        return False
    return True


def _inside_trailing_window(
    value: Any, start: datetime | None, end: datetime | None
) -> bool:
    if (
        not isinstance(value, datetime)
        or not isinstance(start, datetime)
        or not isinstance(end, datetime)
    ):
        return True
    return start <= value < end


def _limit_allows(side: str, price: Decimal, limit: Decimal) -> bool:
    return price <= limit if side == "BUY" else price >= limit


def _exec_price(side: str, historical_price: Decimal, buffer: Decimal) -> Decimal:
    price = historical_price + buffer if side == "BUY" else historical_price - buffer
    return max(Decimal("0"), min(Decimal("1"), price)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _price_bucket(price: Decimal) -> str:
    value = _decimal(price)
    if value < Decimal("0.10"):
        return "0.00-0.10"
    if value < Decimal("0.25"):
        return "0.10-0.25"
    if value < Decimal("0.50"):
        return "0.25-0.50"
    if value < Decimal("0.75"):
        return "0.50-0.75"
    if value < Decimal("0.90"):
        return "0.75-0.90"
    return "0.90-1.00"


def _age_seconds(arrival: Any, last: Any) -> Decimal | None:
    if not isinstance(arrival, datetime) or not isinstance(last, datetime):
        return None
    return Decimal(str(max(0.0, (arrival - last).total_seconds()))).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _horizon_seconds(
    order: Any,
    arrival_ts: Any,
    deadline_ts: Any,
    arrival_block: Any,
    deadline_block: Any,
) -> Decimal | None:
    if isinstance(arrival_ts, datetime) and isinstance(deadline_ts, datetime):
        return Decimal(
            str(max(0.0, (deadline_ts - arrival_ts).total_seconds()))
        ).quantize(Q, rounding=ROUND_HALF_UP)
    if arrival_block is not None and deadline_block is not None:
        return Decimal(max(0, int(deadline_block) - int(arrival_block))).quantize(
            Q, rounding=ROUND_HALF_UP
        )
    horizon = getattr(order, "horizon", None)
    if isinstance(horizon, timedelta):
        return Decimal(str(max(0.0, horizon.total_seconds()))).quantize(
            Q, rounding=ROUND_HALF_UP
        )
    return None


def _max_dt(left: datetime | None, right: Any) -> datetime | None:
    if not isinstance(right, datetime):
        return left
    if left is None or right > left:
        return right
    return left


def _ratio(actual: Any, required: Any) -> Decimal:
    req = _decimal(required)
    if req <= 0:
        return Decimal("1")
    act = _decimal(actual)
    return max(Decimal("0"), min(Decimal("1"), act / req)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value.total_seconds())
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value
