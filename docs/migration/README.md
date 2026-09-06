# PMQ Extraction

Target: `/home/jiahuaiyu/develop/polymarket/repos/backtest-lab`.

## Selection Rules

`inventory.json` records source hashes and the reason each file was selected.
The migration includes the full research-engine module tree and its Python
dependency closure. Tests are selected by imports, not their old directory:
Paper and ingestion tests were previously mixed into `quant/backtest/tests`.
They stay with their original owner.

The HTTP blueprint retains the original backtest handler bodies. Market-price,
tile/stream, product-hub and LOB data-service routes are removed from this
blueprint. The old combined API is not rewritten during extraction.

## Cleanup

- The entire vendored Backtrader copy is replaced by pinned `backtrader==1.9.78.123`.
- Rust `target`, Python caches, bulk experiment outputs and raw L2/trade archives
  are not source dependencies and are not imported into this repository.
- Fixed-cohort `compare_real_execution_models.py`,
  `compare_stratified_real_execution_models.py` and
  `compare_pml2_event_driven_real_markets.py` are superseded for routine use by
  the parameterized `batch_compare_real_execution_models.py`.
- Reusable tests, financial settlement, source provenance and model artifacts
  are preserved. Old production/readiness modules are still HTTP dependencies;
  their size alone is not a sufficient reason to delete them.

## Safety Boundary

The existing Paper service remains in PMQ until the separate `paper-trading`
migration is accepted. Its archive replay and paired-probe dependencies must
continue resolving the same backtest contract types.

At extraction time an Alpha acceptance process still references the old PMQ
checkout. Source-path-sensitive running work must not be redirected mid-run.
Deletion/cutover of active imports is a separate step from a verified copy.
Only backed-up, inactive experiment artifacts may be removed from the old tree.

No production database mutations, live orders, service restarts or old-result
reinterpretation are part of this migration.

## Verification (2026-09-06)

- Extracted repository regression, including retained strategies:
  **1,370 passed, 3 skipped**, 28.62 seconds.
  The three optional DB smoke tests were not enabled; Rust tests did not skip.
- Rust release unit tests: **4 passed**, built from this checkout.
- Existing Paper baseline: **87 passed**.
- Existing Paper with this checkout's `quant.backtest` explicitly injected:
  **87 passed**, with socket network access prohibited. This verifies local
  contract/lifecycle compatibility, not remote Paper production health.
- Real ClickHouse canary: **10,000 trades / 100 orders**, V2 and V3
  source-confirmed Python/Rust results and capacity hashes match.
- Pinned installed Backtrader completed a buy/sell replay without the vendor.
- One older strategy test used `__dict__` on a slotted `V2TradePrint`; the fixture
  now uses `dataclasses.replace`. Strategy and matching behavior are unchanged.
- V2 replay, V3 engine/models, PML2 session/book and Rust kernel source match the
  original files byte for byte. All **16 execution-model JSONs** also match.
- Focused Ruff and staged whitespace checks passed. The full legacy source tree
  was not reformatted or claimed to be globally lint-clean.
- `http://127.0.0.1:18504` serves the extracted API. Health, V2/V3/PML2 profiles,
  and actual ClickHouse coverage readiness returned HTTP 200.

The API blueprint retains 64 research routes, removing 1,881 lines of unrelated
route/helper code while adding 32 lines of extracted imports/layout. No matching
probability, TIF, participation or freshness parameter was relaxed.

## Cleanup Receipt

Five completed acceleration runs plus three obsolete fixed-cohort scripts were
relocated outside the old repository: **254 files, 1,249,294,320 bytes**.
Recovery archive:
`/home/jiahuaiyu/develop/polymarket/migration-archive/backtest-lab-20260906`.
Original paths and hashes are in its `cleanup.json`; this is reversible removal
from the working tree, not destruction of the only data copy or reclaimed disk.

The initial import is roughly 14 MB of source, models, documentation and tests.
Raw market archives, model-provenance datasets, Paper account records and active
Alpha acceptance work remain in their original locations. `quant/benchmark`
golden/live-shadow suites that import PaperLedger also remain Paper-owned;
they are not silently converted into backtest-only acceptance tests.

## Deferred Cutover

`quant/backtest` has **not** been deleted from PMQ. An existing PMQ065 Alpha
process (PID 1880091 at verification) binds to old source paths, and Paper/data
consumers have not yet moved into their empty destination repositories. The new
repository is independently runnable; old-source removal is not yet complete.

After those consumers finish or migrate, retarget their declared source root,
rerun `tools/verify_legacy_paper.py`, then archive the old source according to
`inventory.json`. Do not substitute two independently editable implementations
or change imports in an already running source-identified experiment.
