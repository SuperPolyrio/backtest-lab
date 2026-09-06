# Fill-only V2 Replay API

This API exposes the existing OrderFilled V2 trade-tape matcher to sibling
research programs. It accepts structured taker orders only. It does not accept
Python code, read LOB data, write backtest tables, or calculate settlement PnL.

Base URL on the local service:

```text
http://127.0.0.1:18500/quant/fill-only/v2
```

## Endpoints

```text
GET  /profiles
GET  /readiness
POST /resolve-anchor
POST /replay
```

`GET /readiness` must be checked before an experiment. Coverage comes from
completed `orderfilled_v2_build_chunks` receipts for
`trade_prints_one_sided`. A replay window outside those receipts fails with
HTTP 409; it is never reported as `NO_FILL`.

The replay endpoint supports two compatible order-time contracts:

- timestamp-native: `signalTs` is required and `signalBlock` is omitted;
- legacy dual-axis: both `signalBlock` and `signalTs` are supplied.

Timestamp-native replay does not resolve or interpolate one block anchor per
order. It derives arrival/deadline windows directly from `signalTs`, loads
`block_time` slices in batches, and retains `sourceMaxBlock` only as the frozen
run-level source pin.

## Replay request

```json
{
  "requestId": "alpha-formal-v2-001",
  "profile": "conservative_trade_tape",
  "sourceMaxBlock": 89592478,
  "defaultLookbackBlocks": 100,
  "defaultHorizonBlocks": 100,
  "maxRowsPerWindow": 1000,
  "mergeGapBlocks": 0,
  "orders": [
    {
      "orderId": "intent-0001",
      "marketId": 2416400,
      "assetId": "e8cdbee07212d978ea47de16d33a9b1744135aa1ab6126b98520b9f201b817a5",
      "side": "BUY",
      "limitPrice": "0.57",
      "size": "10",
      "signalBlock": 89502028,
      "signalTs": "2026-07-02T10:11:38Z",
      "tif": "GTC",
      "allowPartialFill": true,
      "signalSourceTradeId": null,
      "signalSourceTxHash": null,
      "signalSourceLogIndexes": []
    }
  ]
}
```

These values are the frozen real-data acceptance fixture. For the order shown
above they load 15 `trade_prints_one_sided` rows and return
`PARTIAL_FILLED 5.3728152 / 10`. Using `2000 / 2000` is a different experiment:
it loads about 290 rows and fills `10 / 10`, so those parameters must not be
presented as evidence for the 15-row partial-fill result.

`signalTs` is required. `signalBlock` remains supported for old callers but is
optional. When supplied, the block bounds the ClickHouse slice and the
timestamp enforces arrival latency. When omitted, the service uses the
timestamp-native contract below.

Set `signalSourceTradeId` or its transaction/log identity when an OrderFilled
event created the signal. Profiles that set `exclude_signal_source_trade=true`
then prevent that event from also proving execution.

The API applies one shared capacity ledger to all orders in request order. Use
unique order IDs and preserve the strategy's causal order.

## Timestamp-native replay

This is the preferred contract for chronological strategy backtests:

```json
{
  "requestId": "pmq073-time-native-001",
  "profile": "probabilistic_trade_tape",
  "sourceMaxBlock": 90792478,
  "defaultLookbackSeconds": 300,
  "defaultHorizonSeconds": 300,
  "mergeGapSeconds": 0,
  "maxRowsPerWindow": 100000,
  "orders": [
    {
      "orderId": "intent-001",
      "marketId": 2416400,
      "assetId": "e8cdbee07212d978ea47de16d33a9b1744135aa1ab6126b98520b9f201b817a5",
      "side": "BUY",
      "limitPrice": "0.57",
      "size": "10",
      "signalTs": "2026-07-02T10:11:38Z",
      "tif": "GTC",
      "allowPartialFill": true
    }
  ]
}
```

The service performs one run-level `trade_time` envelope check for the whole
request. The response records it as
`time_coverage_envelope.validation_method=run_level_trade_time_envelope` and
`is_order_anchor=false`. This proves that completed receipt coverage and real
source trades enclose the requested time range; it does not manufacture a
`signalBlock` and is not the `/resolve-anchor` interpolation contract.

Timestamp windows always include both predicates:

```text
source receipt block interval <= sourceMaxBlock
AND start_ts <= block_time <= end_ts
```

Overlapping windows for the same market, asset and side are merged before
ClickHouse loading. Matching still requires a real post-arrival source trade,
limit compatibility, named-profile capacity, TIF, and the request-ordered
shared capacity ledger.

Optional per-order overrides are `latencySeconds`, `horizonSeconds`, and
`lookbackSeconds`. IOC/FOK/FAK still have a one-second effective timestamp
deadline.

## Resolve a causal block anchor

The resolver remains available for legacy callers that explicitly need a block
anchor:

```json
{
  "requestId": "anchor-intent-0001",
  "signalTs": "2026-07-02T10:11:38Z",
  "sourceMaxBlock": 89592478,
  "maxDistanceSeconds": 120
}
```

```text
POST /quant/fill-only/v2/resolve-anchor
```

The resolver brackets `signalTs` with the nearest real
`trade_prints_one_sided` rows inside the pinned source range and returns either
an exact block or a causal block interpolation. The response includes both
source trade IDs, block times, distances, method and `coverage_eligible`.

It deliberately does not use LOB or silently fall back to an unverified block
estimate. Missing brackets, excessive distance, non-monotonic evidence or a
timestamp outside the derived trade-tape range returns HTTP 409.

## Curl example

```bash
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  --data @request.json \
  http://127.0.0.1:18500/quant/fill-only/v2/replay
```

The static workbench proxy also exposes:

```text
http://127.0.0.1:3100/wm-api/quant/fill-only/v2/replay
```

## Response contract

Important fields are:

```text
schema_version
manifest_hash
execution_model
execution_grade
uses_lob_data=false
profile
source_max_block
source_coverage
time_coverage_envelope
trade_slice_load
match_diagnostics
summary
orders[].fills[].source_trade_id
orders[].fills[].source_tx_hash
orders[].fills[].source_log_indexes
capacity_ledger
market_window_capacity_ledger
manifest
```

The response uses strings for decimal values. Preserve them as decimal values;
do not convert them through binary floats.

The replay reports execution cash and position deltas. Strategy PnL remains a
separate layer:

```text
settlement payout - actual fill cost - fees - capital cost
```

`NO_FILL` has zero realized strategy PnL. A counterfactual immediate-fill PnL
may be reported separately but must not be included in the primary result.

## Profiles

The default is `conservative_trade_tape`. The profile endpoint lists all
available OrderFilled-only profiles, including probabilistic sensitivity
profiles. The LOB-holdout-calibrated profile is deliberately excluded from
this API because this contract is restricted to OrderFilled-only assumptions.

Profile parameters are not accepted as arbitrary request overrides. Select a
named profile and freeze it before OOS. Block lookback/horizon fields define
legacy dual-axis slices; second-based fields define timestamp-native slices.
Changes must be recorded in the returned manifest.

### OrderFilled probability and penetration profiles

`GET /profiles` exposes machine-readable probability metadata including
`probability_model_version`, `min_fill_probability`,
`hard_reject_below_probability`, `capacity_variant`,
`trade_side_evidence_mode` and `fills_require_source_trade`.

The probability profiles are:

| Profile | Horizon | Evidence | Conditional capacity | Use |
|---|---:|---|---|---|
| `probabilistic_conservative` | 300s | same-side print | 25% quantile | conservative probability lower bound |
| `probabilistic_trade_tape` | 300s | same-side print | expected fraction | central strategy-research estimate |
| `probabilistic_source_confirmed` | 300s | same-side print | 100% of source participation cap | auditable upper bound |
| `probabilistic_taker_30s` | 30s | same-side print | expected fraction | short taker sensitivity |
| `probabilistic_taker_120s` | 120s | same-side print | expected fraction | wider taker sensitivity |
| `probabilistic_taker_30s_any_order_side` | 30s | real price penetration on either aggressor side | expected fraction | trade-side-uncertain sensitivity |
| `probabilistic_taker_120s_any_order_side` | 120s | real price penetration on either aggressor side | expected fraction | wider trade-through sensitivity |

The model estimates `p_fill` from pre-arrival OrderFilled tape features. It
does not perform a random Bernoulli fill and does not create liquidity. Every
fill still needs a post-arrival source trade, a limit-compatible execution
price and available shared participation capacity. The `any_order_side`
profiles relax aggressor-side evidence only; they still require a real trade
print at a qualifying price.

IOC, FOK and FAK retain a one-second effective deadline regardless of the
selected profile. Use a preregistered GTC/GTD order when testing a 30s, 120s or
300s penetration horizon. Do not silently reinterpret an IOC as a longer-lived
order.

Example expected-capacity request:

```json
{
  "requestId": "probability-expected-001",
  "profile": "probabilistic_trade_tape",
  "sourceMaxBlock": 90792478,
  "defaultLookbackBlocks": 300,
  "defaultHorizonBlocks": 2000,
  "maxRowsPerWindow": 100000,
  "orders": [
    {
      "orderId": "intent-001",
      "marketId": 2416400,
      "assetId": "e8cdbee07212d978ea47de16d33a9b1744135aa1ab6126b98520b9f201b817a5",
      "side": "BUY",
      "limitPrice": "0.57",
      "size": "10",
      "signalBlock": 89502028,
      "signalTs": "2026-07-02T10:11:38Z",
      "tif": "GTC",
      "allowPartialFill": true
    }
  ]
}
```

Each order response includes `p_fill`, `conditional_capacity_fraction`,
`fill_capacity_variant`, `fill_probability_eligibility`, model features and
the frozen probability profile.

## Strict JSON contract

Unknown fields are rejected at both the request and order levels. For example,
top-level `lookbackBlocks` is invalid; the supported request field is
`defaultLookbackBlocks`, while an order-specific override is `lookbackBlocks`
inside one order. A misspelling such as `limitPrce` also returns HTTP 400.

Snake-case and camel-case aliases are supported, but sending both aliases for
the same value is rejected as ambiguous. This prevents a misspelled or ignored
parameter from silently changing replay results.

## Failure states

| HTTP | `error_code` | Meaning |
|---:|---|---|
| 400 | `INVALID_FILL_ONLY_REQUEST` | Invalid order, profile, timestamp, side, price, or TIF |
| 409 | `TRADE_TAPE_COVERAGE_GAP` | Requested window is outside completed V2 build receipts |
| 409 | `TRADE_TAPE_ANCHOR_UNRESOLVABLE` | Decision time lacks a nearby causal two-sided trade-tape bracket |
| 413 | `FILL_ONLY_REQUEST_LIMIT_EXCEEDED` | Too many orders or a trade slice would be truncated |
| 503 | `TRADE_TAPE_UNAVAILABLE` | ClickHouse or V2 build receipts are unavailable |

A 409/413/503 response is a technical/data failure, not a clean no-fill.

The exact-RPC catch-up completed the frozen PMQ-065 Formal V2 decision window
on 2026-08-24. Completed trade-tape receipts are contiguous through block
90,792,478 (`2026-07-24T11:48:34Z`). The six preregistered decision hours from
`2026-07-21T14:00:00Z` through `2026-07-24T07:00:00Z` all resolve over the real
HTTP endpoint with status 200 and `coverage_eligible=true`.

This is not a claim that the entire raw OrderFilled tail is derived. Blocks
90,792,479 through the frozen raw upper bound remain outside completed V2
receipts and must still return HTTP 409. Do not weaken the check or convert a
coverage failure into `NO_FILL`.

## Server limits

Defaults are bounded for synchronous research calls:

```text
orders per request:       100
rows per merged window:   100,000
total loaded trade rows:  500,000
maximum block horizon:    100,000
maximum time horizon:     100,000 seconds
```

They can be reduced or raised by service environment variables:

```text
POLYDATA_QUANT_FILL_ONLY_API_MAX_ORDERS
POLYDATA_QUANT_FILL_ONLY_API_MAX_ROWS_PER_WINDOW
POLYDATA_QUANT_FILL_ONLY_API_MAX_TOTAL_TRADE_ROWS
POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_BLOCKS
POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_SECONDS
```

If a valid experiment exceeds these bounds, split it into deterministic,
non-overlapping batches and retain the same frozen `sourceMaxBlock` and profile.
