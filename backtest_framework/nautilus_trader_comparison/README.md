# External Framework Execution Comparison

Date: 2026-08-25

## Tested software

- NautilusTrader source: `develop@7dcd5274feb4`
- NautilusTrader runtime: Conda `polymonitor-nautilus312`, package `1.227.0`
- polymarket-toolkit: `main@4a66e7d6b15b`
- prediction-market-quant: current working tree

The latest Nautilus `develop` source was inspected but not rebuilt because this host has no `cargo`,
`rustc`, or `uv`. Runtime tests use the installed stable package and its matching source tag.

## NautilusTrader results

The detailed 12-order comparison between the TradeTick/GTC event-replay path and the
arrival-time static-L2/FOK path is recorded in
[`nautilus_tradetick_gtc_vs_static_l2_fok.md`](./nautilus_tradetick_gtc_vs_static_l2_fok.md).
The original static-L2 `12/12` result bypassed PML2 and was initially treated as an optimistic
upper bound. The frozen twelve-order cohort has now been replayed through the full event-driven
PML2 state machine: every observed execution variant filled `12/12` orders and 120 shares, matching
the static Nautilus result on this cohort.

## PML2 event-driven book validity

PML2 now uses exchange-style event validity by default. A synchronized reconstructed book remains
valid until a gap, clear/reset, required resnapshot, unverified handover, pause, cancel-only state,
or close explicitly invalidates it. Elapsed wall time without a book change is not itself an
invalidation event. The old `max_book_age_ms` behavior remains available as a differential control
through `BookValidityMode.MAX_AGE`.

A frozen real-data comparison used ten distinct markets from sports, politics, crypto, tech, and
economy. Each market contributed an independent BUY-at-best-ask and SELL-at-best-bid FOK order,
for 20 orders total. All three paths consumed the same strictly reconstructed L2 levels:

| Model | Orders with fill | Filled size |
| --- | ---: | ---: |
| PML2 legacy 5-second max-age control | 6/20 | 30 |
| PML2 event-driven validity | 20/20 | 100 |
| Nautilus static L2 | 20/20 | 100 |

PML2 event-driven and Nautilus matched fill quantity and average price on `20/20` orders. The
fourteen recovered orders had unchanged-book ages from about 6 to 166 seconds, with no known
invalidating event after the reconstructed state. Two additional frozen markets remained rejected:
one lacked a valid book baseline and one failed the authoritative top-hint fence. Thus the change
removes elapsed-time false negatives without relaxing archive integrity or explicit gap gates.

Reproduction:

```bash
python scripts/compare_pml2_event_driven_real_markets.py
```

The complete market names, source clocks, levels, coverage windows, per-order fills, and negative
controls are in
[`pml2_event_driven_real_markets.json`](./pml2_event_driven_real_markets.json).

`nautilus_binary_option_smoke.py` uses a BinaryOption, TradeTicks, one BUY limit for 10 shares,
and no order-book data.

| Scenario | Result |
| --- | --- |
| Resting exact touch, `prob_fill_on_limit=0` | no fill |
| Resting exact touch, `prob_fill_on_limit=1` | full fill 10 |
| 10,000 direct probability draws at 25% | 2,513 successes, 25.13% |
| Strict trade-through, probability 0 | full fill 10 |
| Marketable on signal print, zero latency | full fill 10 on the signal print |
| Marketable on signal print, one-second latency, no future print | no fill |
| Order arrival and touch share the same second | full taker fill even with touch probability 0 |

The final row matters for whole-second Polymarket Data API timestamps: event ordering can turn an
otherwise passive touch into an immediately marketable taker fill. This is looser, but not
necessarily more accurate.

## polymarket-toolkit results

- TypeScript tests: 64 passed.
- Python pagination/checkpoint tests: 13 passed.
- TypeScript typecheck: passed.
- Live `markets --limit 2 --active`: passed with `NODE_USE_ENV_PROXY=1`.
- Live markout: 5 wallet fills, 1,000 comparison prints, 100% coverage.
- Sample excess markout: -13.8331 cents at 10 seconds and -4.6406 cents at 30 seconds.

The toolkit is usable for public API inspection, wallet cashflow, maker/taker mix and execution
quality. It still has no historical order simulation loop, so it cannot replace Fill-only V2/V3.

## Controlled comparison against Fill-only

On the same touch/through tape:

| Model | Filled size |
| --- | ---: |
| V2 same-side exact touch | 0 |
| V2 any-side exact touch, no buffer/probability gate | 2.5 |
| V3 touch-survival expected | 0.5946059625 |
| V3 strict trade-through lower | 1.0 |
| V3 synthetic q50 with no future trade | 2.5 modeled |
| V3 Monte Carlo with no future trade | 0.24 expected, 9.4% fill probability |
| Nautilus exact touch at probability 1 | 10 |
| Nautilus strict trade-through | 10 |

Nautilus allocates the full order from a 100-share print. Fill-only applies a 1%-2.5%
participation cap and, in V2 probabilistic profiles, may apply an additional probability capacity
multiplier.

## Real tape comparison

Frozen market/token:

- market: `3841892`
- asset: `e80ed3b32a72333e92bab97cea46266e05841a9945bd7a1c19b85bff060947cc`
- 40 BUY orders, each 10 shares
- limit: signal trade price plus 0.01, capped at 0.99
- one-second latency, 30-second/100-block horizon
- signal source trade excluded

### Active window

Blocks `91593000-91600000`, 6,432 trade rows:

| Profile | Orders with any fill | No fill | Total filled size |
| --- | ---: | ---: | ---: |
| V2 probabilistic 30s | 37/40 | 3 | 299.6256462010 |
| V3 source-confirmed | 37/40 | 3 | 324.6227758250 |
| V3 hierarchical expected 30s | 40/40 modeled | 0 | 346.3783223587 expected |
| V3 hierarchical expected 120s | 40/40 modeled | 0 | 363.2639781866 expected |
| V3 hierarchical expected 300s | 40/40 modeled | 0 | 363.4465250656 expected |
| V2 optimistic | 40/40 | 0 | 390.7033845500 |

### Sparse window

Blocks `91552000-91592000`, 298 trade rows:

| Profile | Orders with any fill | No fill | Total filled size |
| --- | ---: | ---: | ---: |
| V2 probabilistic 30s | 5/40 | 35 | 7.8909292930 |
| V2 probabilistic 30s any-side | 5/40 | 35 | 5.5117346700 |
| V3 source-confirmed | 5/40 | 35 | 13.7793366750 |
| V3 synthetic q50 | 4/40 | 36 | 7.9006267000 |
| V3 Monte Carlo positive expected fill | 4/40 | 36 | 0.6371908670 |
| V3 hierarchical expected 30s | 25/40 modeled | 15 | 36.8930167210 expected |
| V3 hierarchical expected 120s | 25/40 modeled | 15 | 105.3793391906 expected |
| V3 hierarchical expected 300s | 25/40 modeled | 15 | 155.6052379058 expected |
| V2 optimistic | 34/40 | 6 | 223.4266657500 |

## Diagnosis

1. The main no-fill cause is the hard requirement for a post-arrival eligible print inside a short
   horizon. Any-side evidence did not improve the sparse cohort.
2. IOC is even stricter because it clamps the effective horizon to one second.
3. A 0.005 adverse price buffer rejects exact-limit touches. In the controlled case, any-side still
   failed until the buffer was removed.
4. The built-in V2 probability profile is marked `builtin_untrained` with `training_rows=0`.
5. V2 applies pre-arrival fill probability to capacity even after a real source trade is observed.
   V3 source-confirmed kept the same no-fill count but allocated 74.6% more size in the sparse
   cohort, indicating a double conservatism in V2 conditional capacity.
6. V3 synthetic and Monte Carlo paths do not solve very sparse windows: 25/40 orders lacked the
   minimum pre-arrival tape, while another 11/40 failed the inferred execution-price limit.
7. Nautilus is looser because it permits full-print capacity and same-timestamp marketability. With
   whole-second timestamps this can overfill, so copying it directly would trade one bias for another.

The new hierarchical expected profiles implement the previously missing split:

```text
expected fill = P(fill by horizon) * E(fill fraction | fill) * order size
```

They reuse the 23,982-sample OrderFilled probability/capacity calibration and shrink sparse local
windows toward a pooled trade-anchored prior. They still require at least one pre-arrival trade.
They do not require a future source trade. In the paired sparse cohort, they convert 21 of the 35
V2 `NO_FILL` orders (60%) into explicitly labeled modeled expectations at the 30-second horizon;
14 remain `NO_FILL` because no pre-arrival trade anchors the model. Those 21 orders carry 31.478
expected shares at a mean modeled fill probability of 21.32%. As a retrospective diagnostic, 11
of the 21 later find source-confirmed evidence within 300 seconds. That 52.38% support rate is not
an execution label for the original 30-second window, but it shows the widened cases are not all
detached from subsequent real tape activity.

This is not a new historical fill claim. The trained label is future source-event availability,
not actual submitted-order execution. Until validated on real orders including no-fills, the API
labels these profiles `ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED` and keeps their PnL
separate from source-confirmed PnL. The 120/300-second profiles are horizon sensitivities; selecting
the most profitable horizon after seeing strategy PnL would be invalid.
