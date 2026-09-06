# Backtest Lab

Polymarket research replay extracted from `prediction-market-quant`.

## Scope

- OrderFilled-only V2: source evidence, participation, TIF, shared capacity.
- Trade-only V3: probability, hierarchical priors, modeled quantities and RNG.
- PML2: L2 snapshot/delta/trade replay, taker and maker, coverage and queues.
- Oracle financial finalization, reusable validation, catalogs and Rust kernels.
- Existing `/quant/backtest-runs` and standalone replay HTTP contracts.

Modeled expected fills are not observed executions. Offline validation is not
live-order calibration. No historical results are used as implicit model weights.

## Local Installation

Use a source checkout: model registries and SQL remain repository-relative.

```bash
conda activate polyBacktest
python -m pip install -e '.[test]'
python -m pip install ./rust/fill_only_kernel
python scripts/quant_api_server.py --port 18504 --skip-init-schema
```

Do not copy production credentials into this repository. Supply PostgreSQL,
ClickHouse and archive settings through the existing `POLYDATA_*` / `POLY_*`
environment variables. The API does not initialize a database when started with
the command above. Metadata reads need an explicitly configured data service.

For a local transition, `BACKTEST_LAB_ENV_FILE=/absolute/path/to/private.env`
loads an existing private configuration without copying it. Explicit process
environment values take precedence. Never commit that file.

```text
GET  /quant/health
GET  /quant/fill-only/v2/profiles
POST /quant/fill-only/v2/replay
GET  /quant/fill-only/v3/profiles
POST /quant/fill-only/v3/replay
POST /quant/fill-only/v3/resolve-anchor
GET  /quant/prediction-l2/v2/profiles
POST /quant/prediction-l2/v2/replay
POST /quant/backtest-runs
POST /quant/backtest-runs/<run_id>/finalize
```

## Tests

```bash
python -m pytest
cargo test --release --manifest-path rust/fill_only_kernel/Cargo.toml
```

Database and archive checks need their own data settings. A missing archive is
not a matching-engine failure and is not silently scored as a pass.

## Repository Boundaries

`backtest-lab` owns historical research execution. `market-data` owns collection;
`paper-trading` owns paper/live submission and account services. This extraction
retains the public `quant.backtest.*` imports, plus the dependency-closed shared
data/economics contracts needed to run independently. It does not contain the
Paper API, Paper ledger service, live executor, or L2 collector.

See [the migration record](docs/migration/README.md) for ownership, compatibility,
cleanup decisions and verification. Historical model ideas are retained in
`quantV2/docs`; API documentation is in `docs/api`.
