# Fill-only V3 / Trade-only Execution API

This API is a parallel research model. It does not modify or replace the
source-confirmed Fill-only V2 API.

```text
Fill-only V2: /quant/fill-only/v2/*
Fill-only V3: /quant/fill-only/v3/*
```

Both use `trade_prints_one_sided` and never read runtime LOB data. V3 adds
explicit evidence tiers and model-inferred sensitivity paths. Results from
different tiers must not be merged into one unlabeled equity curve.

## Endpoints

```text
GET  /quant/fill-only/v3/profiles
GET  /quant/fill-only/v3/readiness
POST /quant/fill-only/v3/replay
```

## Evidence tiers

| Tier | Mode | Real source trade | Result role |
|---|---|---:|---|
| A | `SOURCE_CONFIRMED_TAPE` | yes | auditable lower bound |
| B | `PASSIVE_TRADE_THROUGH_LOWER` | strict-cross trigger | inferred passive lower bound |
| C | `PASSIVE_TOUCH_SURVIVAL` | touch/cross trigger | uncalibrated sensitivity |
| D | `SYNTHETIC_ARRIVAL_LIQUIDITY` | no | model-inferred sensitivity |
| E | `GENERATIVE_TAPE_MC` | generated paths | distributional sensitivity |

Tier A delegates to the unchanged V2 matcher. Tier B distinguishes passive
queue evidence from taker participation:

```text
resting BUY:  SELL aggressor at limit = touch; below limit = strict cross
resting SELL: BUY aggressor at limit = touch; above limit = strict cross
```

Tier B fills at the resting order limit, not the later cross price. Tier C
reports first-touch/first-cross interval bounds and deterministic expected
capacity. Its hazard is explicitly `INTERVAL_HEURISTIC_UNCALIBRATED` until
real order labels are available.

Tier D estimates a latent midpoint, effective spread, q10/q50/q90 arrival
capacity and tick-space impact from pre-arrival tape. Its fills have empty
`source_trade_ids`, empty `source_tx_hashes`, a feature snapshot hash and the
`D_SYNTHETIC_ARRIVAL` evidence tier. They are not historical fill claims.

Tier E uses a seeded Poisson arrival proxy and returns fill probability,
full-fill probability, expected quantity and q10/q50/q90 quantity. Repeating
the same manifest and seed is deterministic.

`HIERARCHICAL_EXPECTED_FILL` is an additional Tier D screening path. It keeps
the occurrence and size contracts separate:

```text
expected_fill_size
    = P(fill by horizon)
    * E(fill fraction | fill)
    * requested_size
```

The 30-second occurrence model and conditional fraction model were fitted on
23,982 OrderFilled-only samples. Sparse local windows shrink toward the pooled
trade-anchored prior. At least one real pre-arrival trade is required; no
post-arrival source trade is required. This is a transfer from prediction of a
future source event to a latent execution expectation, so its status is
`ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED`, not audit-grade execution.
It fills at the order limit as a worst-price rule and never emits a source tx.

## Profiles

```text
taker_source_confirmed
maker_trade_through_lower
maker_touch_survival_conservative
maker_touch_survival_expected
maker_touch_survival_upper
taker_synthetic_q10
taker_synthetic_q50
taker_synthetic_q90
generative_tape_mc
taker_hierarchical_expected_30s
taker_hierarchical_expected_120s
taker_hierarchical_expected_300s
auto_bound
```

The hierarchical profiles default to GTD and explicit 30/120/300-second
horizons. They reject IOC/FAK/FOK because a deterministic fractional
expectation is not a valid immediate-or-atomic order outcome. API responses
separately expose:

```text
p_fill_horizon
conditional_fill_fraction
conditional_fill_size
unconditional_expected_fill_size
local_weight / prior_weight
probability_training_rows
expected_fill_is_observed_execution = false
```

`auto_bound` returns three separate scenarios:

```text
strict_lower
expected_modeled
synthetic_upper
```

It deliberately returns `BOUND_SET`, not a single historical execution truth.
The top-level `filled_size` is zero because no mixed-evidence scenario is
selected for accounting. If the uncalibrated scenarios do not satisfy
`lower <= expected <= upper`, the status is `BOUND_ORDERING_VIOLATION` and the
response includes `bound_diagnostics`; callers must not treat it as an interval.

## Request

```json
{
  "requestId": "trade-only-v3-001",
  "profile": "taker_synthetic_q50",
  "sourceMaxBlock": 90792478,
  "defaultLookbackBlocks": 300,
  "defaultHorizonBlocks": 2000,
  "maxRowsPerWindow": 100000,
  "randomSeed": 73,
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
      "tif": "IOC",
      "liquidityIntent": "TAKER",
      "allowPartialFill": true
    }
  ]
}
```

The API requires both `signalBlock` and `signalTs`. Block bounds make the
ClickHouse slice efficient; timestamps enforce causal arrival ordering. The
timestamp remains the on-chain block/log timestamp, not the unobserved CLOB
match timestamp.

## TIF boundary

For `IOC`, `FAK` and `FOK`, the source-tape mode retains the existing
one-second tape proxy and labels it `ONE_SECOND_TAPE_PROXY`. This is not a
claim that actual arrival depth was observed.

Passive trade-through and touch-survival profiles return:

```text
UNOBSERVABLE_IMMEDIATE_LIQUIDITY
```

for immediate TIF orders. Synthetic arrival can model immediate capacity.
FOK is atomic: capacity at least requested size produces a full fill;
otherwise it produces no fill. Monte Carlo reports FOK path probabilities,
never a fractional single-path FOK fill.

## Capacity and audit

Orders in one request share a run liquidity ledger. Separate ledgers track:

```text
V2 source-confirmed capacity
trade-through/touch source capacity
synthetic arrival capacity by market/token/side/arrival second
```

The service is stateless across HTTP requests. Put competing orders in one
request or use deterministic non-overlapping batches.

Unknown JSON fields and conflicting snake/camel aliases return HTTP 400.
Incomplete derivation coverage returns HTTP 409. Slice truncation returns
HTTP 413. These failures are not `NO_FILL`.

## Research boundary

Only `taker_source_confirmed` is an audit-grade source-confirmed result.
Trade-through is a conservative inference. Touch-survival, synthetic-arrival
and generative-MC profiles remain sensitivity models until calibrated against
point-in-time real orders that include both fills and no-fills.

The hierarchical expected profiles are suitable for stable, large-sample alpha
screening. They must be paired with source-confirmed and Monte Carlo results for
final strategy evaluation; their fractional expected quantity is not an
historical order lifecycle claim.

Primary strategy reporting should retain separate columns for:

```text
source-confirmed PnL
trade-through lower PnL
expected modeled PnL
synthetic upper PnL
```

Profile selection must be frozen before out-of-sample strategy evaluation.
