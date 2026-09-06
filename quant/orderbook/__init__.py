"""Local order book state machines for Polymarket CLOB data.

Public symbols are loaded lazily so lightweight control-plane commands do not
initialize PyArrow's machine-wide worker pool.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS = {
    "BookLevel": (".local_book", "BookLevel"),
    "BookMetrics": (".local_book", "BookMetrics"),
    "LocalOrderBook": (".local_book", "LocalOrderBook"),
    "OrderBookNotReady": (".local_book", "OrderBookNotReady"),
    "OrderBookOutOfOrder": (".local_book", "OrderBookOutOfOrder"),
    "TokenBookIdentity": (".local_book", "TokenBookIdentity"),
    "NormalizedBookDelta": (".polymarket_adapter", "NormalizedBookDelta"),
    "NormalizedBookSnapshot": (".polymarket_adapter", "NormalizedBookSnapshot"),
    "normalize_polymarket_event": (".polymarket_adapter", "normalize_polymarket_event"),
    "normalize_rest_book": (".polymarket_adapter", "normalize_rest_book"),
    "book_snapshot_from_local_book": (".backtest", "book_snapshot_from_local_book"),
    "build_lob_execution_coverage_report": (".coverage", "build_lob_execution_coverage_report"),
    "BookQualityAssessment": (".book_quality", "BookQualityAssessment"),
    "assess_book_quality": (".book_quality", "assess_book_quality"),
    "compare_local_to_rest": (".book_quality", "compare_local_to_rest"),
    "quality_from_metrics": (".book_quality", "quality_from_metrics"),
    "BookStore": (".book_store", "BookStore"),
    "fetch_gate_b_status": (".book_store", "fetch_gate_b_status"),
    "insert_snapshot_rows": (".book_store", "insert_snapshot_rows"),
    "L2DepthGateDecision": (".l2_coverage_gate", "L2DepthGateDecision"),
    "L2DepthIntervalGateDecision": (".l2_coverage_gate", "L2DepthIntervalGateDecision"),
    "evaluate_l2_depth_gate": (".l2_coverage_gate", "evaluate_l2_depth_gate"),
    "evaluate_l2_depth_interval_gate": (".l2_coverage_gate", "evaluate_l2_depth_interval_gate"),
    "build_postgres_snapshot_row": (".sinks", "build_postgres_snapshot_row"),
    "RealtimeBookOutput": (".service", "RealtimeBookOutput"),
    "RealtimeBookTarget": (".service", "RealtimeBookTarget"),
    "RealtimeOrderBookService": (".service", "RealtimeOrderBookService"),
    "should_persist_snapshot": (".storage_policy", "should_persist_snapshot"),
    "DesiredSubscription": (".subscription_reconciler", "DesiredSubscription"),
    "SubscriptionReconcileResult": (".subscription_reconciler", "SubscriptionReconcileResult"),
    "SubscriptionReconciler": (".subscription_reconciler", "SubscriptionReconciler"),
    "build_realtime_targets_from_desired": (".subscription_reconciler", "build_realtime_targets_from_desired"),
    "load_desired_subscriptions": (".subscription_reconciler", "load_desired_subscriptions"),
    "MarketSubscriptionCandidate": (".subscriptions", "MarketSubscriptionCandidate"),
    "SubscriptionDecision": (".subscriptions", "SubscriptionDecision"),
    "build_subscription_candidates_from_rows": (".subscriptions", "build_subscription_candidates_from_rows"),
    "score_subscription_candidate": (".subscriptions", "score_subscription_candidate"),
    "select_subscription_tokens": (".subscriptions", "select_subscription_tokens"),
    "token_shard": (".subscriptions", "token_shard"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value
