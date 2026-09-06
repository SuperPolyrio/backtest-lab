# V3/PML2 acceleration validation

Current reproducible result:

```text
input: 10 non-overlapping windows, 12,500 orders, 366 markets
models: V3 source + two V3 central paths + PML2 + Nautilus control
old end-to-end: 3265.924854 s
current end-to-end: 57.90 s
speedup: 56.41x
peak RSS: 348,028 KiB
per-order mismatches across all five models: 0
```

The final acceleration run is in `cross_validation_12500_final/`. Its historical
`FAIL` belongs to the probability artifact active for that run, not to performance
or replay equivalence. The replacement artifact later reached
`PASS_WITH_CALIBRATION_WARNINGS` on 12,276 external-validation orders; failed local
calibration strata and abstentions remain visible in that validation artifact.

PML2 `auto` uses the measured faster backend. For the 1,250-order real windows,
bulk Python is faster than the Rust bridge; Rust remains available for large,
shallow batches and explicit differential checks.

Dynamic PML2 was profiled separately on 50 markets, 78,393 real compact L2 events,
and 11,000 GTD maker orders. Removing per-event whole-book hashing and indexing
waiting orders by condition reduced the practical `CHAIN_ONLY` run to 20.080 s;
the no-audit control was 14.333 s with an identical per-order/final-book digest.
The Python event loop is therefore retained. Evidence and profiles are under
`../pml2_dynamic_profile/`.
