# PML2 dynamic replay profile

This directory measures the Python snapshot/delta/trade/maker-queue/GTD path.

## Corpus

- 50 real markets and 16 categories from the 2026-07-16 compact L2 slice
- 78,393 real snapshot, delta, and trade events
- 11,000 GTD maker orders
- 206,455 scheduled exchange/local/order envelopes

The compact cache preserves event and receive clocks but lacks complete raw-frame
proof. It is a research performance corpus, not formal archive-coverage evidence.

## Result

| Mode | Replay | Ingest + submit + replay |
| --- | ---: | ---: |
| `CHAIN_ONLY` | 20.0797 s | 24.8674 s |
| no-audit diagnostic | 14.3332 s | 18.9289 s |

Both modes produced the same 11,000 order results, 26 matches, final residual book,
and execution digest:

```text
6a3554f1b2c706c50c62795d741d6cfae7aa67b273a4a55969d5e1c0af8926ec
```

A 60-order/1,249-event direct control measured `FULL=14.3897s` and
`CHAIN_ONLY=0.2520s`, a 57.11x replay speedup with identical execution state.

Reproduce:

```bash
/home/jiahuaiyu/.conda/envs/polyBacktest/bin/python \
  scripts/benchmark_pml2_dynamic_replay.py \
  --market-limit 50 \
  --orders-per-market 220 \
  --window-seconds 300 \
  --modes chain_only,no_audit \
  --output backtest_framework/nautilus_trader_comparison/pml2_dynamic_profile/large_11000_final.json
```

Current decision: keep dynamic matching in Python. The measured remaining Python
event loop is too small to justify a Rust bridge now; re-profile before revisiting.

The main local research endpoint persists and accepts `pml2AuditMode`. It defaults
to `CHAIN_ONLY`; use `FULL` explicitly when every event payload must be retained.
The standalone PML2 V2 replay API keeps its audit-first `FULL` default.
