# Fill-first 外部源配置说明

当前回测主线是 Fill-first / OrderFilled-calibrated execution。核心回测模型可以在本地用 `orderfilled_fact`、成交回放、fill report、calibration 和 promotion gate 跑通；但要进入 paper/live 可信阶段，还必须接入真实外部证据源。

## 为什么必须接外部源

这些源不是 LOB/DEPTH，也不是价格模型本身，而是用来验证“模拟成交是否接近真实执行”，以及“策略使用的外部信号是否有可审计的观测时间、延迟和 payload 指纹”：

| 外部源 | 作用 | 没有它时的影响 |
| --- | --- | --- |
| real order state | 对齐订单 submit、accepted、filled、cancel、reject 状态 | 只能做历史 fill replay，不能校准真实延迟、cancel race、ghost fill、post-only reject |
| real cost events | 对齐 fee、rebate、gas、redeem、settlement、capital cost | PnL 只能用模拟成本，不能证明实盘净收益和账本一致 |
| platform incidents | 对齐 CLOB/Gamma/API 维护、异常、service not ready 时间段 | 无法解释某些 No Fill、延迟或异常成交是不是平台状态导致 |
| external signal events | 对齐策略外部信号的 observed_at/block、latency、payload_hash、resolution/oracle/source | 策略可能看起来用了“当时可见”的信息，但无法证明信号不是未来函数、延迟未被低估、resolution 口径未混淆 |

所以 quality gate 出现 `review external order-state source`、`review external wallet/cost source`、`review external platform incident source` 或 `review external signal source` 时，含义是：代码链路已经有，但真实私有数据源还没有配置。

## 配置入口

已有两个 env 示例。生产启动检查优先使用统一四源模板，单独的 order-state collector 模板用于长期轮询 worker：

- `deploy/systemd/fill-first-external-sources.env.example`
  - `ORDER_STATE_INPUT`
  - `ORDER_STATE_API_URL`
  - `ORDER_STATE_AUTH_HEADER`
  - `ORDER_STATE_KEY`
  - `COST_EVENTS_INPUT`
  - `COST_EVENTS_URL`
  - `PLATFORM_INCIDENTS_INPUT`
  - `PLATFORM_INCIDENTS_URL`
  - `EXTERNAL_SIGNAL_INPUT`
  - `EXTERNAL_SIGNAL_URL`

- `deploy/systemd/real-order-state-collector.env.example`
  - `ORDER_STATE_API_URL`
  - `ORDER_STATE_AUTH_HEADER`
  - `ORDER_STATE_SINCE_PARAM`
  - `ORDER_STATE_CURSOR_PARAM`
  - `ORDER_EXECUTION_SUBMIT_URL`

真实部署时复制到本地私有路径，例如：

```bash
/etc/prediction-market-quant/real-order-state-collector.env
/etc/prediction-market-quant/fill-first-external-sources.env
```

不要把真实 token、URL、钱包导出路径提交进仓库。

## 本地验证命令

没有真实源时，先用 fixture 验证导入链路：

```bash
conda run -n polyBacktest python scripts/run_fill_first_external_source_fixture_pipeline.py --check-db --rollback-smoke
```

如果要让 fixture 直接对齐某次真实回测 run 的订单 ID、market、token 和模拟成交状态，可以从已落库 run 生成 run-specific 证据包：

```bash
conda run -n polyBacktest python scripts/write_fill_first_external_source_fixture.py \
  --use-latest-fill-first-run \
  --output-dir /tmp/fill_first_run_fixture \
  --source-prefix run-fixture \
  --overwrite
```

这个入口会读取 `quant.quant_backtest_orders`，生成订单状态 JSONL、成本 JSONL、platform incident JSONL、external signal JSONL，并写出可直接传给统一导入器的 env-file。它的用途是验证“某次回测订单 -> shadow/live 证据 -> calibration/import 链路”以及“策略外部信号 -> run artifact 审计链路”是否能按同一组 `run_id/order_id/market/token` 对齐；它仍然是本地 fixture，不代表真实私有订单 API、钱包流水、平台异常源或真实外部信号源。

更强的阶段自检可以在一个数据库事务里导入这份 run-specific fixture，构造 calibration samples，再回滚，不留下测试行：

```bash
conda run -n polyBacktest python scripts/run_fill_first_external_source_fixture_pipeline.py \
  --use-latest-run-id \
  --from-run-orders \
  --rollback-smoke \
  --build-calibration-smoke \
  --output-dir /tmp/fill_first_run_fixture_pipeline \
  --overwrite
```

这一步证明的不只是文件格式能读，而是同一批 `quant_backtest_orders` 可以和外部订单状态 evidence 匹配，进入 `quant.quant_backtest_calibration_orders` 的样本构造逻辑。rollback smoke 会先确保 `quant` schema/tables 存在，再在同一个事务里写入 fixture evidence 并回滚测试行；要把真实证据长期写库，需要接入真实外部源后再显式 `--write`。

run-specific fixture pipeline 现在还会输出 `run_coverage`。这个报告会直接检查当前 run 的 `order_state_coverage_pct` 和 `calibration_coverage_pct`，避免只看到“文件导入成功”，但实际订单没有被真实状态或 calibration sample 覆盖。

如果确认要把某次 run-specific fixture 写入数据库用于页面/API 调试，可以显式打开写入和 calibration 构造：

```bash
conda run -n polyBacktest python scripts/run_fill_first_external_source_fixture_pipeline.py \
  --use-latest-run-id \
  --from-run-orders \
  --write \
  --check-db \
  --build-calibration \
  --output-dir /tmp/fill_first_run_fixture_pipeline \
  --overwrite
```

这个命令会把 order-state、cost、incident、external signal 写入对应 evidence 表，并用导入后的 order-state 构造 `quant.quant_backtest_calibration_orders`；cost evidence 也会尝试和 ledger 构造 `quant.quant_backtest_cost_calibration`。写入后 `scripts/audit_backtest_run_artifacts.py --run-id <run_id>` 和前端 Fill Quality 才能读到实际 calibration sample 数和 external signal contract。这个模式会留下测试 evidence 行，只应该用于明确的调试 run；生产可信阶段仍要替换成真实订单 API、真实钱包/成本源、真实 incident 源和真实 external signal 源。

要单独验证 paper/live preflight 的配置检查链路，但不调用任何真实订单 API，可以跑本地 production preflight fixture：

```bash
conda run -n polyBacktest python scripts/run_fill_first_production_preflight_fixture.py \
  --output-dir /tmp/fill_first_production_preflight_fixture \
  --overwrite
```

这个命令会生成本地 order-state、cost、incident、external signal fixture 文件，并使用 `127.0.0.1` 的 paper dry-run submit URL 检查 `audit_external_source_env.py`、`check_fill_first_production_readiness.py` 和 order execution safety。它只证明配置检查和 dry-run safety 链路完整，不代表真实私有源已经接入。

检查某个 run 是否真的被外部证据覆盖，用 run 级覆盖报告：

```bash
conda run -n polyBacktest python scripts/check_external_source_run_coverage.py --run-id <run_id>
```

它会同时检查外部订单状态覆盖率、calibration sample 覆盖率、真实成本事件、成本校准样本、平台 incident 重叠和 external source import state。这个检查解决的是“全局外部源看起来有导入，但当前这个回测 run 其实没有 evidence/calibration”的问题。

Promotion gate 会使用这个 run 级覆盖结果。`order_state_coverage_pct` 或 `calibration_coverage_pct` 不是 `100`，或者 missing evidence plan 里还有缺失订单时，策略只能继续留在 backtest/补证据阶段，不能推进 paper/live。

如果覆盖率是 `review`，先导出缺失证据工作单：

```bash
conda run -n polyBacktest python scripts/export_external_source_missing_evidence_plan.py \
  --run-id <run_id> \
  --format markdown
```

更推荐从 run artifact audit 直接导出完整任务包：

```bash
conda run -n polyBacktest python scripts/audit_backtest_run_artifacts.py \
  --run-id <run_id> \
  --export-missing-evidence-dir runtime_outputs/missing_external_evidence_<run_id>
```

这个目录会同时包含：

- `missing_external_evidence_<run_id>.json`：完整缺失证据 plan。
- `missing_external_evidence_<run_id>.md`：人工可读工作单。
- `missing_external_evidence_<run_id>.event_templates.jsonl`：待补真实 order-state evidence 模板。
- `missing_external_evidence_<run_id>.commands.sh`：后续 validate、import、build calibration、check coverage 命令。

补齐 JSONL 里的 `event_time`、真实终态、fill price/size、fee/rebate、cash/position delta、latency 等字段后，先跑任务包 pipeline 的 dry-run：

```bash
conda run -n polyBacktest python scripts/run_missing_external_evidence_task_pack.py \
  --task-pack-dir runtime_outputs/missing_external_evidence_<run_id> \
  --events runtime_outputs/missing_external_evidence_<run_id>/filled_events.jsonl
```

确认 validation 通过后，再显式写库并刷新 calibration/coverage：

```bash
conda run -n polyBacktest python scripts/run_missing_external_evidence_task_pack.py \
  --task-pack-dir runtime_outputs/missing_external_evidence_<run_id> \
  --events runtime_outputs/missing_external_evidence_<run_id>/filled_events.jsonl \
  --run-id <run_id> \
  --state-key real-order-state-task-pack-<run_id> \
  --write
```

这一步会依次执行：validate shadow/live event -> import `quant.real_order_state_events` -> build `quant.quant_backtest_calibration_orders` -> re-check run-level external evidence coverage。默认不写库，只有加 `--write` 才会改变数据库。

需要给真实订单 exporter 或人工补字段时，导出只包含缺 order-state 的 JSONL 模板：

```bash
conda run -n polyBacktest python scripts/export_external_source_missing_evidence_plan.py \
  --run-id <run_id> \
  --format event-jsonl \
  --output runtime_outputs/missing_external_evidence_<run_id>.jsonl
```

这个文件只是待补工作单，不是真实 evidence。补齐 `event_time`、真实终态、fill price/size、fee/rebate、cash/position delta、latency 等字段后，再走 `validate_shadow_live_order_events.py -> import_real_order_state_events.py -> build_backtest_calibration_samples.py -> check_external_source_run_coverage.py`。

配置真实 env 后，先审计配置：

```bash
conda run -n polyBacktest python scripts/audit_external_source_env.py --env-file /etc/prediction-market-quant/fill-first-external-sources.env --strict-review
```

如果要把这些 `review` 直接变成生产接入清单，先跑 onboarding plan：

```bash
conda run -n polyBacktest python scripts/plan_external_source_onboarding.py \
  --env-file /etc/prediction-market-quant/fill-first-external-sources.env \
  --format markdown \
  --strict-review
```

这个计划会按 `real_order_state_events`、`real_cost_events`、`platform_incidents`、`external_signal_events` 四类源分别输出：

- 当前 file/url/unconfigured 模式。
- 缺哪些 env key。
- 生产完成标准。
- masked dry-run 命令。
- write 命令预览。
- 仍然阻止 paper/live 的 production blockers。

它不调用真实外部 API，也不会写库；用途是让外部源接入从“缺配置”变成可以逐项完成和复查的工作单。

进入 paper/live 前，生产预检建议同时指定当前要推进的回测 `run_id`：

```bash
conda run -n polyBacktest python scripts/check_fill_first_production_readiness.py \
  --target-mode paper \
  --env-file /etc/prediction-market-quant/fill-first-external-sources.env \
  --check-db \
  --run-id <run_id>
```

带 `--run-id` 后，preflight 不只检查外部源配置和 import freshness，还会检查该 run 的 `order_state_coverage_pct` 和 `calibration_coverage_pct` 是否都是 `100`。如果 task pack 还没补齐或 calibration 没构造完成，`launch_allowed` 会被阻断。

再跑导入 dry-run：

```bash
conda run -n polyBacktest python scripts/run_configured_external_source_imports.py --env-file /etc/prediction-market-quant/fill-first-external-sources.env
```

确认无误后才加 `--write`。

## 当前判断标准

- readiness 允许 `review`，但不能有 `missing`。
- quality gate 允许真实源未配置导致的 `review`，但不能有 `fail`。
- 要进入 paper/live 可信阶段，外部源至少要有持续导入水位，并通过 `quant.external_source_import_state` freshness 检查；使用外部世界/新闻/模型信号的策略，还必须让 `external_signal_events` 进入 run artifact 的 signal contract report。

每个开发阶段的默认验收命令：

```bash
conda run -n polyBacktest python scripts/run_fill_first_quality_gate.py --stage-check --check-db --format markdown
```

如果本机还没有外部源 env-file，可以先生成 file-mode 骨架：

```bash
conda run -n polyBacktest python scripts/bootstrap_fill_first_external_sources.py \
  --target-dir .local/fill-first-external-sources \
  --format markdown
```

这个命令会创建四个空 JSONL 文件和一个本地 env-file，并输出 audit/import/quality gate 命令。它只证明本机 file-mode 外部证据链路准备好了，`contains_real_evidence=false`，不能当成生产真实订单/成本/incident/signal 证据。

如果本机还没有订单执行 env-file，可以先生成 paper-only dry-run 骨架：

```bash
conda run -n polyBacktest python scripts/bootstrap_fill_first_order_execution.py \
  --target-dir .local/fill-first-order-execution \
  --format markdown
```

这个命令会生成 `ORDER_EXECUTION_TARGET_MODE=paper` 和本机占位 submit URL，用于审计 `ORDER_EXECUTION_*` 配置和 dry-run adapter 命令。报告里会标记 `contains_live_execution=false`、`contains_secret=false`；它不会启用 live 下单，也不会替代真实订单 API 回执证据。真正调用外部订单 API 仍然必须在 adapter 里显式传 `--execute`，并满足 `--record-events`、auth、live confirm 等安全门禁。

外部源 env-file 和订单执行 env-file 可以分开传给质量门禁；外部源负责 evidence 导入，订单执行 env 只负责 adapter/preflight：

```bash
conda run -n polyBacktest python scripts/run_fill_first_quality_gate.py \
  --external-env-file .local/fill-first-external-sources/fill-first-external-sources.env \
  --order-execution-env-file .local/fill-first-order-execution/order-execution-paper.env \
  --stage-check \
  --format markdown
```

`--stage-check` 会跑 Phase 1 doc alignment、external source env bootstrap fixture、order execution env bootstrap fixture、fixture、external source onboarding fixture、parameter search plan fixture、parameter search results fixture、parameter search batch fixture、parameter search scheduler fixture、production parameter staging fixture、production parameter staging review fixture、current schema run artifact fixture、`quant/backtest/tests` 和前端 build；主 quality gate 也会输出 `external source onboarding` gate，把真实 env 的 production blockers、缺失 key 和下一步命令放进 payload/next_actions。带 `--check-db` 时还会跑最新 fill-first run artifact audit、run-level 外部证据覆盖率审计和外部源 rollback DB smoke。rollback DB smoke 会先拿 schema advisory lock，再初始化 `quant` schema、写入 fixture 证据并回滚，避免并发门禁和 DDL/fixture 写入互相死锁。需要单独检查 Phase 1 文档验收映射时，运行 `conda run -n polyBacktest python scripts/check_phase1_doc_alignment.py --strict --format markdown`。需要生成真实 current-schema 样本时，用 `scripts/run_fill_first_sample_backtest.py --dry-run` 先看候选窗口，再去掉 `--dry-run` 创建 `ORDERFILLED_CROSS` run 并立刻输出 artifact audit 摘要。只要没有 `fail`，本地 fill-first 链路就是可继续研究的；如果仍是 `review`，优先看是否只是缺真实外部源、真实订单执行 endpoint、数据窗口质量不足，或当前样本只适合 backtest 研究而不能 promotion。
