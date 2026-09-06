"""Stable load-aware assignment for full-universe L2 websocket collectors."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import math
from typing import Iterable, Mapping, Protocol


class SubscriptionLike(Protocol):
    asset_id: str
    condition_id: str
    execution_eligible: bool


@dataclass(frozen=True, slots=True)
class TokenLoad:
    events_per_second_60s: float = 0.0
    events_per_second_15m: float = 0.0
    bytes_per_second_60s: float = 0.0
    snapshot_bytes_p95: float = 0.0
    reconnect_snapshot_cost: float = 0.0

    @property
    def weight(self) -> float:
        return max(
            1.0,
            self.bytes_per_second_60s
            + 256.0
            * max(self.events_per_second_60s, self.events_per_second_15m)
            + 0.02 * self.snapshot_bytes_p95
            + 0.01 * self.reconnect_snapshot_cost,
        )


@dataclass(frozen=True, slots=True)
class AssignmentPlan:
    assignments: dict[str, int]
    shard_weights: tuple[float, ...]
    shard_token_counts: tuple[int, ...]
    worker_weights: tuple[float, ...]
    worker_token_counts: tuple[int, ...]
    moved_tokens: int
    new_tokens: int
    maximum_to_median_ratio: float
    maximum_tokens_per_shard: int | None
    salt: str


def plan_weighted_shards(
    entries: Iterable[SubscriptionLike],
    *,
    shard_count: int,
    loads: Mapping[str, TokenLoad] | None = None,
    previous: Mapping[str, int] | None = None,
    salt: str = "source-a",
    maximum_migration_ratio: float = 0.01,
    rebalance_threshold_ratio: float = 1.5,
    maximum_tokens_per_shard: int | None = None,
    worker_count: int | None = None,
    worker_rebalance_threshold_ratio: float = 1.05,
) -> AssignmentPlan:
    """Plan stable LPT assignments with condition affinity as a soft grouping.

    Existing tokens stay put unless the current maximum shard weight exceeds
    the median by ``rebalance_threshold_ratio``. New tokens do not consume the
    migration budget. When rebalancing, the highest-cost beneficial moves are
    applied first and the number of moved existing tokens is capped.
    """

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if worker_count is None:
        worker_count = shard_count
    worker_count = int(worker_count)
    if worker_count <= 0 or worker_count > shard_count:
        raise ValueError("worker_count must satisfy 1 <= worker_count <= shard_count")
    worker_grouping_enabled = worker_count < shard_count
    if not 0 <= maximum_migration_ratio <= 1:
        raise ValueError("maximum_migration_ratio must satisfy 0 <= ratio <= 1")
    if worker_rebalance_threshold_ratio < 1:
        raise ValueError("worker_rebalance_threshold_ratio must be >= 1")
    if maximum_tokens_per_shard is not None:
        maximum_tokens_per_shard = int(maximum_tokens_per_shard)
        if maximum_tokens_per_shard <= 0:
            raise ValueError("maximum_tokens_per_shard must be positive")
    loads = loads or {}
    previous = previous or {}
    normalized = tuple(entries)
    if (
        maximum_tokens_per_shard is not None
        and len(normalized) > shard_count * maximum_tokens_per_shard
    ):
        raise ValueError(
            "maximum_tokens_per_shard has insufficient total capacity"
        )
    ranked_hot = sorted(
        (
            entry
            for entry in normalized
            if entry.asset_id in loads
        ),
        key=lambda entry: (
            -loads.get(entry.asset_id, TokenLoad()).weight,
            entry.asset_id,
        ),
    )
    hot_count = min(
        len(ranked_hot),
        max(1, math.ceil(len(normalized) * 0.001))
        if normalized and ranked_hot
        else 0,
    )
    hot_assets = {
        entry.asset_id for entry in ranked_hot[:hot_count]
    }
    by_condition: dict[str, list[SubscriptionLike]] = defaultdict(list)
    for entry in normalized:
        condition = (
            str(entry.condition_id or "").strip() or str(entry.asset_id)
        )
        # Condition affinity is deliberately soft: the hottest 0.1% are
        # singleton groups so complementary outcomes cannot jointly saturate
        # one socket. The long cold tail retains locality.
        key = (
            f"hot:{entry.asset_id}"
            if entry.asset_id in hot_assets
            else f"condition:{condition}"
        )
        by_condition[key].append(entry)

    groups: list[tuple[float, str, tuple[SubscriptionLike, ...]]] = []
    for key, members in by_condition.items():
        group_weight = sum(
            loads.get(member.asset_id, TokenLoad()).weight for member in members
        )
        groups.append((group_weight, key, tuple(members)))
    groups.sort(key=lambda item: (-item[0], _salted_rank(item[1], salt)))

    ideal: dict[str, int] = {}
    ideal_weights = [0.0] * shard_count
    ideal_counts = [0] * shard_count
    ideal_worker_weights = [0.0] * worker_count
    ideal_worker_counts = [0] * worker_count
    for group_weight, key, members in groups:
        candidates = sorted(
            range(shard_count),
            key=lambda shard_id: (
                ideal_worker_counts[shard_id % worker_count],
                ideal_worker_weights[shard_id % worker_count],
                ideal_weights[shard_id],
                ideal_counts[shard_id],
                _salted_rank(f"{key}:{shard_id}", salt),
            ),
        )
        fitting = [
            shard_id
            for shard_id in candidates
            if maximum_tokens_per_shard is None
            or ideal_counts[shard_id] + len(members)
            <= maximum_tokens_per_shard
        ]
        if fitting:
            target = fitting[0]
            for member in members:
                ideal[member.asset_id] = target
            ideal_weights[target] += group_weight
            ideal_counts[target] += len(members)
            ideal_worker_weights[target % worker_count] += group_weight
            ideal_worker_counts[target % worker_count] += len(members)
            continue
        # Keep condition affinity until it would violate the connection token
        # ceiling; split only that group across shards with spare capacity.
        for member in sorted(members, key=lambda value: value.asset_id):
            member_weight = loads.get(member.asset_id, TokenLoad()).weight
            target = min(
                (
                    shard_id
                    for shard_id in range(shard_count)
                    if maximum_tokens_per_shard is None
                    or ideal_counts[shard_id] < maximum_tokens_per_shard
                ),
                key=lambda shard_id: (
                    ideal_worker_counts[shard_id % worker_count],
                    ideal_worker_weights[shard_id % worker_count],
                    ideal_weights[shard_id],
                    ideal_counts[shard_id],
                    _salted_rank(f"{member.asset_id}:{shard_id}", salt),
                ),
            )
            ideal[member.asset_id] = target
            ideal_weights[target] += member_weight
            ideal_counts[target] += 1
            ideal_worker_weights[target % worker_count] += member_weight
            ideal_worker_counts[target % worker_count] += 1

    current = {
        entry.asset_id: int(previous[entry.asset_id])
        for entry in normalized
        if entry.asset_id in previous
        and 0 <= int(previous[entry.asset_id]) < shard_count
    }
    assignments = dict(current)
    current_weights = [0.0] * shard_count
    current_counts = [0] * shard_count
    current_worker_weights = [0.0] * worker_count
    current_worker_counts = [0] * worker_count
    for entry in normalized:
        shard_id = current.get(entry.asset_id)
        if shard_id is None:
            continue
        weight = loads.get(entry.asset_id, TokenLoad()).weight
        current_weights[shard_id] += weight
        current_counts[shard_id] += 1
        current_worker_weights[shard_id % worker_count] += weight
        current_worker_counts[shard_id % worker_count] += 1
    new_tokens = 0
    for entry in normalized:
        if entry.asset_id not in assignments:
            target = ideal[entry.asset_id]
            if (
                maximum_tokens_per_shard is not None
                and current_counts[target] >= maximum_tokens_per_shard
            ):
                target = min(
                    (
                        shard_id
                        for shard_id in range(shard_count)
                        if current_counts[shard_id] < maximum_tokens_per_shard
                    ),
                    key=lambda shard_id: (
                        current_weights[shard_id],
                        current_counts[shard_id],
                        _salted_rank(f"new:{entry.asset_id}:{shard_id}", salt),
                    ),
                )
            assignments[entry.asset_id] = target
            current_weights[target] += loads.get(
                entry.asset_id,
                TokenLoad(),
            ).weight
            current_counts[target] += 1
            current_worker_weights[target % worker_count] += loads.get(
                entry.asset_id,
                TokenLoad(),
            ).weight
            current_worker_counts[target % worker_count] += 1
            new_tokens += 1
    current_ratio = _maximum_to_median_ratio(current_weights)

    moved = 0
    count_overloaded = (
        maximum_tokens_per_shard is not None
        and max(current_counts, default=0) > maximum_tokens_per_shard
    )
    worker_count_overloaded = (
        worker_grouping_enabled
        and _maximum_to_median_ratio(current_worker_counts)
        > float(worker_rebalance_threshold_ratio)
    )
    if current and (
        current_ratio > float(rebalance_threshold_ratio)
        or count_overloaded
        or worker_count_overloaded
    ):
        migration_budget = max(
            1,
            int(len(current) * float(maximum_migration_ratio)),
        )
        candidates: list[tuple[int, float, str, int, int]] = []
        for entry in normalized:
            old = current.get(entry.asset_id)
            target = ideal[entry.asset_id]
            if old is None:
                continue
            weight = loads.get(entry.asset_id, TokenLoad()).weight
            pressure = int(
                maximum_tokens_per_shard is not None
                and current_counts[old] > maximum_tokens_per_shard
            )
            worker_pressure = int(
                worker_grouping_enabled
                and old % worker_count != target % worker_count
                and current_worker_counts[old % worker_count]
                > current_worker_counts[target % worker_count] + 1
            )
            if old == target and not pressure and not worker_pressure:
                continue
            benefit = current_weights[old] - current_weights[target]
            if not pressure and not worker_pressure and benefit <= weight:
                continue
            candidates.append(
                (
                    2 if pressure else 1 if worker_pressure else 0,
                    weight if worker_pressure and not pressure else -weight,
                    entry.asset_id,
                    old,
                    target,
                )
            )
        candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
        for _pressure, ranked_weight, asset_id, old, ideal_target in candidates:
            if moved >= migration_budget:
                break
            weight = abs(ranked_weight)
            pressure = (
                maximum_tokens_per_shard is not None
                and current_counts[old] > maximum_tokens_per_shard
            )
            worker_pressure = (
                worker_grouping_enabled
                and old % worker_count != ideal_target % worker_count
                and current_worker_counts[old % worker_count]
                > current_worker_counts[ideal_target % worker_count] + 1
            )
            if pressure or worker_pressure:
                targets = [
                    shard_id
                    for shard_id in range(shard_count)
                    if shard_id != old
                    and (
                        not worker_pressure
                        or current_worker_counts[shard_id % worker_count]
                        < current_worker_counts[old % worker_count]
                    )
                    and (
                        maximum_tokens_per_shard is None
                        or current_counts[shard_id]
                        < maximum_tokens_per_shard
                    )
                ]
                if not targets:
                    break
                target = min(
                    targets,
                    key=lambda shard_id: (
                        current_worker_counts[shard_id % worker_count],
                        current_worker_weights[shard_id % worker_count],
                        current_weights[shard_id],
                        current_counts[shard_id],
                        0 if shard_id == ideal_target else 1,
                        _salted_rank(f"move:{asset_id}:{shard_id}", salt),
                    ),
                )
            else:
                target = ideal_target
                if old == target:
                    continue
                if (
                    maximum_tokens_per_shard is not None
                    and current_counts[target] >= maximum_tokens_per_shard
                ):
                    continue
            # Earlier moves in this same bounded batch change both sides of
            # later candidates. Re-evaluate against the live weights so a
            # target that has already filled cannot become the new hotspot.
            # The inequality is exactly the condition for this move to reduce
            # the sum of squared shard weights.
            if (
                not pressure
                and not worker_pressure
                and current_weights[old] - current_weights[target] <= weight
            ):
                continue
            assignments[asset_id] = target
            current_weights[old] -= weight
            current_weights[target] += weight
            current_counts[old] -= 1
            current_counts[target] += 1
            current_worker_weights[old % worker_count] -= weight
            current_worker_weights[target % worker_count] += weight
            current_worker_counts[old % worker_count] -= 1
            current_worker_counts[target % worker_count] += 1
            moved += 1

    final_weights = [0.0] * shard_count
    final_counts = [0] * shard_count
    final_worker_weights = [0.0] * worker_count
    final_worker_counts = [0] * worker_count
    for entry in normalized:
        target = assignments[entry.asset_id]
        final_weights[target] += loads.get(
            entry.asset_id,
            TokenLoad(),
        ).weight
        final_counts[target] += 1
        final_worker_weights[target % worker_count] += loads.get(
            entry.asset_id,
            TokenLoad(),
        ).weight
        final_worker_counts[target % worker_count] += 1
    return AssignmentPlan(
        assignments=assignments,
        shard_weights=tuple(final_weights),
        shard_token_counts=tuple(final_counts),
        worker_weights=tuple(final_worker_weights),
        worker_token_counts=tuple(final_worker_counts),
        moved_tokens=moved,
        new_tokens=new_tokens,
        maximum_to_median_ratio=_maximum_to_median_ratio(final_weights),
        maximum_tokens_per_shard=maximum_tokens_per_shard,
        salt=str(salt),
    )


def _maximum_to_median_ratio(weights: Iterable[float]) -> float:
    ordered = sorted(float(value) for value in weights)
    if not ordered:
        return 0.0
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / 2.0
    )
    return max(ordered) / max(1.0, median)


def _salted_rank(value: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()
