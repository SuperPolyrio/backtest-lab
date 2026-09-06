import json

from quant.backtest.order_state import normalize_order_state_event
from quant.backtest.shadow_live_plan import (
    PLAN_SCHEMA_VERSION,
    build_shadow_live_order_plan,
    shadow_live_event_templates_jsonl,
    shadow_live_order_plan_to_markdown,
)


def sample_inputs() -> dict:
    return {
        "run": {
            "run_id": 42,
            "market_slug": "demo-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
            "from_block": 100,
            "to_block": 200,
            "meta": {"token_id": "token-yes", "parameter_fingerprint": "fp-demo"},
        },
        "parameters": {
            "execution_price_mode": "ORDERFILLED_LIMIT_REPLAY",
            "execution_profile": "realistic",
            "order_role": "taker",
            "latency_blocks": 1,
            "position_size": "10",
        },
        "orders": [
            {
                "order_id": "sim-1",
                "signal_index": 1,
                "signal_x": 101,
                "submit_x": 102,
                "decision_price": "0.52",
                "requested_price": "0.53",
                "requested_size": "10",
                "requested_notional": "5.3",
                "filled_size": "6",
                "filled_notional": "3.18",
                "avg_fill_price": "0.53",
                "fill_probability": "0.8",
                "fill_pct": "60",
                "side": "BUY",
                "role": "taker",
                "order_type": "marketable_limit",
                "status": "PARTIAL_FILLED",
                "execution_source": "orderfilled_limit_replay",
                "meta": {
                    "external_order_id": "live-1",
                    "token_id": "token-yes",
                    "strategy_intent": {
                        "signal": {
                            "strategy_name": "demo-strategy",
                            "metadata": {"signal_time": "2026-06-25T12:00:01Z"},
                        }
                    },
                },
            }
        ],
    }


def test_shadow_live_plan_exports_order_templates() -> None:
    plan = build_shadow_live_order_plan(sample_inputs(), source="live-shadow")

    assert plan["schema_version"] == PLAN_SCHEMA_VERSION
    assert plan["status"] == "ready"
    assert plan["order_count"] == 1
    order = plan["orders"][0]
    template = order["event_template"]
    assert order["order_id"] == "sim-1"
    assert order["client_order_id"] == "sim-1"
    assert order["signal_time"] == "2026-06-25T12:00:01Z"
    assert order["signal_block"] == 101
    assert order["submit_block"] == 102
    assert order["intended_price"] == "0.53"
    assert order["intended_size"] == "10"
    assert order["decision_context"]["execution_profile"] == "realistic"
    assert order["decision_context"]["parameter_fingerprint"] == "fp-demo"
    assert template["run_id"] == 42
    assert template["order_id"] == "sim-1"
    assert template["client_order_id"] == "sim-1"
    assert template["external_order_id"] == "live-1"
    assert template["payload"]["simulated_status"] == "PARTIAL_FILLED"
    assert template["payload"]["signal_time"] == "2026-06-25T12:00:01Z"
    assert template["payload"]["submit_block"] == 102
    assert template["payload"]["intended_price"] == "0.53"
    assert template["payload"]["intended_size"] == "10"
    assert template["payload"]["decision_context"]["client_order_id"] == "sim-1"


def test_event_template_is_compatible_with_order_state_normalizer() -> None:
    plan = build_shadow_live_order_plan(sample_inputs(), source="live-shadow")
    template = plan["event_templates"][0]
    template["event_time"] = "2026-06-25T12:00:00Z"
    template["api_order_status"] = "filled"
    template["payload"]["live_fill_price"] = "0.531"
    template["payload"]["live_fill_size"] = "6"

    normalized = normalize_order_state_event(template)

    assert normalized["run_id"] == 42
    assert normalized["order_id"] == "sim-1"
    assert normalized["external_order_id"] == "live-1"
    assert normalized["source"] == "live-shadow"
    assert normalized["api_order_status"] == "filled"


def test_shadow_live_plan_jsonl_and_markdown_outputs() -> None:
    plan = build_shadow_live_order_plan(sample_inputs())
    jsonl = shadow_live_event_templates_jsonl(plan)
    markdown = shadow_live_order_plan_to_markdown(plan)

    rows = [json.loads(line) for line in jsonl.splitlines()]
    assert rows[0]["order_id"] == "sim-1"
    assert "Shadow/Live Collection Plan" in markdown
    assert "sim-1" in markdown
