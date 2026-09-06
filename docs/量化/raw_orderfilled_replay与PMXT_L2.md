# Raw OrderFilled Replay 与 PMXT L2 的必要性

## 目标

这一步的目标不是重新做价格线，而是把回测从“价格看起来到了”推进到“历史上是否真的有成交证据”。

当前价格线 `market_token_block_close` 适合做信号和展示，但它是处理后的 block close / fair price，不等于某个订单一定能成交。回测如果只用这条价格线，会把很多不可成交的机会误判成可成交。

## 为什么先做 raw OrderFilled replay

`orderfilled_fact` 是真实历史成交明细，包含 block、tx、log、token、price、size。它可以回答一个更严格的问题：

```text
我的限价单提交之后，历史上是否真的出现过穿过该限价的成交？
```

因此它必须作为 builtin 回测的第一层执行证据：

- 防止只按 close price 成交造成乐观回测。
- 能解释订单为什么 filled、partial filled 或 no fill。
- 能记录候选成交 `candidate events`，方便复盘。
- 能应用 `latency_blocks`，避免把订单提交前的成交算进去。
- 能输出 raw/fallback，明确这次回测到底有没有用真实成交明细。

这一步完成后，builtin 回测才可以称为 `OrderFilled-first execution replay`，而不只是价格走势回测。

## 但 raw OrderFilled 还不够

raw OrderFilled 只能证明“某个价格附近发生过成交”，不能完整证明“我的挂单一定排到了”。

它缺少：

- 当时盘口 best bid / best ask。
- 每一档深度。
- 同价位前方队列。
- 盘口缺口、重连、stale 状态。
- marketable order 吃多档深度的真实成本。

所以 raw replay 是必要基础，但不是最终形态。它可以做成交证据和保守校验，不能替代完整 L2 order book replay。

## result.json 里的 fill 讨论要落到回测里

`result.json` 里的讨论重点不是“有没有信号”，而是“信号之后到底有没有高质量成交”。这一步开发需要把这些问题显式纳入报告：

- **No Fill**：赚钱盘口经常吃不到，亏钱盘口反而更容易成交；回测必须统计没成交的单。
- **maker/taker 分桶**：GTC 不等于天然 maker，postOnly 才强制 maker；market order 本质是可立即成交的 limit order。
- **延迟与滑点**：taker 可能有 250ms 到数秒延迟，模拟要支持不利成交、1-2c 滑点、成交率折扣。
- **排队与 cancel race**：maker 回测如果不知道队列位置，就容易把排不到的单算成成交。
- **ghost fill / 订单状态不一致**：撤单后又成交、cancel/get_order 状态不一致，必须进入异常口径。
- **markout**：成交后价格是否逆向，是 maker 策略是否被逆向选择的关键指标。
- **fee/rebate**：毛收益不够，账本必须单独记录 fee、rebate、资金占用和 no-fill 成本。
- **环境异常**：CLOB/Gamma/API 状态异常要和策略表现分开记录，避免把平台问题误当成策略问题。

因此短期的 fill quality 报告至少要包含：

```text
maker/taker
filled / partial / no_fill
raw/fallback
latency submit block
slippage / adverse price
candidate events
cancel/order status anomaly
markout
fee/rebate
environment flags
```

## 当前开发覆盖情况

| result.json 讨论点 | 当前状态 | 下一步 |
| --- | --- | --- |
| No Fill 必须统计 | 已有订单状态、`no_fill_reason`，前端 Fill Quality 已按原因/状态分桶；已补 `missed_opportunity_count`、`missed_opportunity_notional_total`、`avg_missed_opportunity_price_move`、按原因汇总的 no-fill cost；已补 `missed_opportunity_buckets` 和 `missed_opportunity_notional_by_bucket`，覆盖 entry/exit、maker/taker、原因、信号强度代理、流动性桶；本阶段已新增 `quant.real_order_state_events`、`quant.real_order_state_collection_state`、导入脚本、增量采集 state-key/cursor/since 水位、采集 health gate、systemd timer 模板和订单 meta 自动附加，可统计 submit rejected、postOnly rejected、cancel accepted/failed、链上已成交但 API 状态滞后；同时新增 `scripts/export_shadow_live_order_plan.py`，可从某次 backtest run 的模拟订单导出 shadow/live 采集模板，避免真实采集侧不知道该对照哪些订单；新增 `scripts/validate_shadow_live_order_events.py`，导入前校验 live status、真实成交价/量、成本、仓位和延迟字段完整性；新增 `scripts/run_shadow_live_calibration_pipeline.py`，校验通过后再一键导入真实状态、构造 calibration samples 并输出报告；本地自动发现入口已能识别 `shadow_live/order_state/real_order` 类文件并展示 validation 状态 | 后续接真实 order stream/API 凭证并启用生产 timer，而不是手工导入 |
| Fill Report 可导出 | 新增 `quant.backtest.fill_report` 和 `scripts/export_backtest_fill_report.py`，可对某个 run 直接导出 Markdown/JSON，集中展示 submitted/filled/no_fill、`no_fill_reason`、missed orders、raw OrderFilled evidence、data quality、真实订单状态、cost/live calibration 缺口 | 后续真实样本导入后，用该报告快速判断回测偏差来自数据、成交模型还是外部环境 |
| raw 成交证据 | 已接 `orderfilled_fact` raw replay，Fill Quality 已展示 `raw_event_count`、`raw_fallback`、`loaded_block_window`、`candidate_event_count`、`consumed_event_count`；已补 `candidate_event_unique_count`、`candidate_event_duplicate_count`、`consumed_event_unique_count`、`consumed_event_duplicate_count`、`counterparty_tag_rate`，并保留跨订单重复消费 anomaly；本阶段新增 `raw_evidence_summary`，明确 canonical key 字段、keyed/keyless 事件数、candidate/consumed size、raw notional、deduped matched notional、strategy requested/available/filled notional | 后续用真实订单回放校准 counterparty 归因质量，区分 maker/taker 归因缺失是数据缺失还是执行模型无法判断 |
| latency | 已有 `latency_blocks`；已让 `latency_seconds` 映射到 timestamp/block submit window，raw replay 会排除秒级提交前成交；Fill Quality 已展示 `avg_effective_latency_x_span`、`max_effective_latency_x_span`；本阶段已支持从 `quant.real_order_state_events` 附加真实 submit/accepted/cancel 时间，并计算 `avg_order_submit_accept_latency_seconds` 和 `avg_cancel_accept_latency_seconds` | 后续用持续采集的真实时间源校准 RPC/API 延迟和链上确认延迟 |
| adverse slippage / 成交率折扣 | `ORDERFILLED` 概率模型已有；本阶段已让 limit replay 使用 `fill_probability_haircut_pct` 折扣可消费历史成交量，并用 `adverse_slippage_cents` 在 limit 内生成不利成交价；Fill Quality 已展示 effective cap、adverse slip、haircut | 继续用真实订单回放校准 maker/taker 的默认 stress 参数，细分信号强弱和市场流动性桶 |
| execution profile matrix | 已新增 `quant.backtest.execution_profile_matrix`，benchmark 默认 profile bundle 已包含 `fast:realistic` 和 `fast:conservative`；报告会检查 required profiles、conservative 相对 realistic 的 PnL/fill-rate degradation，缺 conservative 时阻止 profile promotion；Strategy Tester Benchmark 已展示 required/present/missing profiles、profile rows 和 comparison degradation | 后续用真实 shadow/live 样本校准各 profile 的默认参数 |
| maker/taker | builtin 执行模型已按 role 选择 maker/taker fee，前端可配置并展示；默认执行模式已扶正为 `ORDERFILLED_CROSS`，也就是价格击穿模型，旧 `ORDERFILLED_LIMIT_REPLAY` 仍兼容映射到同一逻辑；前端和结果属性会同时显示 requested/actual execution mode；已让 OrderFilled Cross 区分 maker `post_only_limit`/GTC 和 taker `marketable_limit`/FAK，taker 只吃 submit window 的 raw fill，不再等待未来成交；本阶段已把导入的真实订单状态事件附加到订单 meta，`post_only_rejected`、`submit_rejected`、`service_not_ready_425` 等 flag 可进入 Fill Quality | 后续用真实订单流校准 postOnly reject 频率、marketable order 的实际 submit/accepted 时间 |
| queue / 排队 | 真正 L2 queue-position 暂时排除；已新增 `Maker Queue Uncertainty Report`，在不接 LOB 的前提下，用 OrderFilled-only 证据标记 maker 成交的不确定性：缺队列证据、参与率过高、raw trade 太薄、postOnly reject、cancel race、负 markout，并输出 `queue_uncertainty_verdict`、`risk_orders` 和 `suggested_fill_haircut_pct` | 后续接 PMXT L2 后，再把该报告从“风险审计/保守 haircut”升级成真实 queue position 近似 |
| cancel race / ghost fill | 已作为 `order_anomaly_flags` 进入 Fill Quality，支持 cancel/ghost/status mismatch note、status/size 冲突、重复 raw fill、fallback fill 标记；本阶段已补 `real_order_state_counts`、`real_order_state_flags`、`real_order_state_flag_count`，并通过 `quant.real_order_state_events` 持久化真实 cancel/order 状态，可区分 cancel submitted/accepted/failed、ghost fill、API/chain 状态滞后；`scripts/collect_real_order_state_events.py` 已支持长期增量采集水位 | 后续补真实 cancel/order stream 的生产凭证、部署和告警 |
| markout | 已按 fill 后 1/5/20 bars 与 60/300/1200 seconds 计算并进入 Fill Quality；已补 `adverse_selection_buckets` 和 `adverse_selection_count`，用于统计成交后价格反向移动样本 | 继续按 maker/taker、信号强弱、流动性桶校准 adverse selection，并接真实订单 submit/accepted/cancel 时间 |
| PnL error calibration | live-vs-sim calibration 已新增 `simulated_pnl`、`live_pnl`、`pnl_error` 和 `pnl_mismatch`，periodic calibration report 会输出 `avg_pnl_error`/`max_pnl_error`，shadow/live triangulation 会把 PnL drift 纳入 `fill_model_suspect`；Strategy Tester Fill Quality 已直接展示 shadow price/PnL/latency/cost drift | 后续真实 shadow/live 样本进入后，用 PnL error 区分成交误差、成本误差和结算/ledger 误差 |
| fee/rebate/cost | 已支持 maker/taker fee、maker rebate，进入 order/trade/ledger/Fill Quality；本阶段已补 `gas_cost_per_order`、`settlement_cost`、`redeem_cost`、`capital_cost_bps`，作为独立 `GAS_COST`、`SETTLEMENT_COST`、`REDEEM_COST`、`CAPITAL_COST` ledger events 影响净收益；已新增 `quant.real_backtest_cost_events`、`quant.quant_backtest_cost_calibration` 和 `scripts/import_real_backtest_cost_events.py`，可把真实钱包/订单流水的 fee、rebate、gas、redeem、settlement、capital cost 与模拟 ledger 对齐；API 已补 `/quant/backtest-runs/<run_id>/cost-calibration`，Strategy Tester 的 Fill Quality 已展示 cost samples、amount error、missing live/simulated 和重校准状态；已补 `quant.external_source_import_state`、`scripts/check_external_source_import_health.py`、`scripts/import_discovered_external_sources.py`、`scripts/run_configured_external_source_imports.py` 和 cost import systemd timer 模板，可自动发现本地成本导出文件，也可按生产 env 统一 dry-run/write，并监控真实成本导入 freshness/last_error/rows_written | 后续接真实钱包/订单流水导出源，并用长期样本校准默认 fee tier、gas、redeem、settlement、rebate 实际到账 |
| 平台异常 | 已作为 `environment_flags` 进入 Fill Quality，覆盖 raw fallback、raw empty、event limit、缺 counterparty、价格 gap/jump/coverage；已新增 `quant.platform_incidents` 和 `scripts/import_platform_incidents.py`，可把 CLOB/Gamma/API 维护、异常、service not ready 等外部时间线按 run 的 block/time/market 窗口匹配进 Fill Quality；本阶段已让 incident 导入写入 `quant.external_source_import_state`，并补 `scripts/import_discovered_external_sources.py`、`scripts/run_configured_external_source_imports.py`、platform incident import timer 与 external source health timer 模板 | 后续接真实外部 incident 来源并持续维护异常时间线 |
| 阶段完整性检查 | 已新增 `scripts/check_fill_first_backtest_readiness.py`、`quant.backtest.fill_first_readiness`、`scripts/run_fill_first_quality_gate.py`、`quant.backtest.fill_first_quality_gate`、`scripts/run_fill_first_sample_backtest.py`、`quant.backtest.sample_run`、`scripts/audit_backtest_run_artifacts.py`、`quant.backtest.run_artifacts`、`scripts/export_shadow_live_order_plan.py`、`quant.backtest.shadow_live_plan`、`scripts/validate_shadow_live_order_events.py`、`quant.backtest.shadow_live_validation`、`quant.backtest.calibration_fixture`、`scripts/run_shadow_live_calibration_pipeline.py`、`scripts/run_configured_external_source_imports.py`、`scripts/audit_external_source_env.py`、`scripts/write_fill_first_external_source_fixture.py`、`scripts/run_fill_first_external_source_fixture_pipeline.py`、`quant.backtest.external_source_env_audit`、`quant.backtest.external_source_fixture`、`quant.backtest.external_source_fixture_pipeline` 和 `quant.backtest.shadow_live_pipeline`，默认排除 LOB/DEPTH；readiness 检查 raw replay、No Fill、真实订单状态、shadow/live 采集计划、shadow/live 证据校验、shadow/live calibration fixture、shadow/live calibration pipeline、fill/cost calibration、shadow/live triangulation、promotion gate、current schema sample backtest、平台异常、configured external source runner、external source env audit、external source fixture、external source fixture DB pipeline、API、前端 Fill Quality、requested/actual engine 与 execution mode、systemd 模板、文档和可选 DB 表；quality gate 进一步聚合外部源发现、外部源 env-file 审计、外部源健康状态、configured import env-file、fixture smoke、shadow/live calibration fixture、external source fixture、可选 rollback DB smoke、最新 fill-first run artifact audit、可选 pytest 和前端 build；`--stage-check` 已成为每阶段默认验收入口，会自动包含 pytest 和前端 build，配合 `--check-db` 时再启用最新 fill-first run artifact audit 与外部源 rollback DB smoke；同时内置 current schema run artifact fixture，避免阶段检查完全依赖数据库里刚好存在最新 schema 样本 run；需要真实 DB 样本时，先用 `scripts/run_fill_first_sample_backtest.py --dry-run` 查看候选，再去掉 `--dry-run` 创建 `ORDERFILLED_CROSS` run 并输出 artifact audit 摘要；本地外部源发现覆盖订单状态、成本、incident 和 external signal；本地/manual 订单状态文件导入现在也支持 `--state-key` 并写入 `quant.external_source_import_state`，与 cost/incident/external signal 文件源的 freshness 检查一致；calibration sample 现在会从订单 meta、真实事件 payload 和订单字段提取 `market_category`、liquidity、volatility、time-to-expiry 上下文，并写入 `payload.context`，保证周期 calibration report 的分桶不会在落库后丢失；periodic calibration report 现在会输出结构化 `execution_profile_suggestions`，可直接进入 pending/approved override 流程；approved execution profile override 现在支持 calibrated run 自动优先匹配 bucket override，再 fallback overall override，避免分桶校准只停留在报告层；external source fixture 可在没有真实私有 API 时生成订单状态/成本/incident/external signal JSONL 和 env-file，用于验证文件链路；DB pipeline 可用 `--rollback-smoke` 或 quality gate 的 `--include-external-fixture-db-smoke` 在事务中验证外部证据表约束且不留测试行，也可显式 `--write --check-db` 验证外部证据表和 import state；quality gate 可用 `--external-env-file` 读取生产 env 文件，先输出 `external source env audit` gate，再输出 configured imports，避免配置文件有 placeholder、缺 auth 或坏格式却直接进入导入；run artifact audit 检查参数指纹、数据版本、block/window、data quality、fill quality、orders、ledger、metrics、校准证据、`Run Credibility`、`Shadow/Live Triangulation Report`、`Fill-First Promotion Gate`、`Execution Regime Report` 和 `Tail Risk Report`；triangulation report 会把 fill calibration、cost calibration、真实状态事件和外部源状态合成 `fill_model_suspect` verdict，差异大时优先怀疑 fill model；regime report 按 role/side/liquidity/volatility/time-to-expiry/no-fill reason 拆分订单生命周期，tail risk report 按 closed trade PnL 输出 payoff distribution、最大单笔亏损、连续亏损压力、losses-to-ruin、position concentration 和 0.95+ 高价成交样本，避免高胜率策略只看 win rate；API 已暴露 `/backtest-runs/<run_id>/artifact-audit`，Strategy Tester 会在 Fill Quality 面板显示 run credibility、shadow/live triangulation、promotion gate、regime report 和 tail risk | 每个阶段结束后都跑一次；readiness 不能有 `missing`，quality gate 不能有 `fail`；旧 run 缺 `fill_quality` 时可用 `--repair-fill-quality` 从已落库 orders 重建 |

补充：run artifact audit 现在还包括 `Settlement Compatibility Report`，记录 resolution source、settlement rule、priceToBeat/oracle source、市场 lifecycle flags、settlement/refund ledger 数量；Strategy Tester 的 Fill Quality 面板同步显示 settlement/source compatibility，避免异常结算源或退款市场被静默当作普通 PnL。

补充：`scripts/export_backtest_fill_report.py` 提供独立 Fill Report 导出，不依赖前端页面。它围绕某个 run 汇总 `no_fill_reason`、missed orders、raw OrderFilled evidence、data quality、真实订单状态和 calibration 缺口，适合在研究阶段快速判断问题来自数据窗口、成交模型、真实执行状态还是外部环境。

补充：run artifact audit 现在还包括独立 `Data Quality Report`，汇总 block/window、row count、gap/max gap、stale、source mix、fallback、dedupe stats、coverage，并输出 ready/review/invalid 结论；Strategy Tester 的 Data Quality 页同步显示当前 run 的 verdict、fallback count 和 duplicate count，避免 source quality 不达标或 fallback 静默进入正式研究结果。

补充：run artifact audit 现在还包括独立 `Reproducibility Report`，检查 code commit、dirty 状态、strategy name/version、parameter fingerprint、data version、block range、source quality、gap report、fill/fee/slippage model version 和 artifact manifest；新 run 创建时会写入这些 provenance 字段，旧 run 缺字段会被标为 missing/review。这个步骤保证 fill-first 回测结果能按同一参数和同一数据窗口回放，不依赖 LOB。

补充：run artifact audit 现在还包括 `Execution/Ledger Parity Report`，检查 backtest 订单生命周期、cashflow ledger 和未来 live/shadow 订单事件是否共享字段 contract；Strategy Tester 会显示 parity、orders schema、ledger schema 和 live evidence。它用于后续 paper/live 对齐和校准 fill probability、slippage、latency，不要求先接 LOB。

补充：run artifact audit 现在还包括 `Ledger Cashflow Validation Report`，用 ledger 的 cash delta、realized PnL 和 cash_after 独立复算 `net_profit_ledger`，再和 trade 表的 `net_profit_trade` 对比 `ledger_diff`。它专门验证“净收益能否从 cashflow ledger 推导”，缺 closed trade 的 ledger rows 或 ledger/trade PnL 不一致会进入 review。Strategy Tester 的 Fill Quality 和 Reproducibility Snapshot 已显示 cashflow verdict、trade net、ledger net、ledger diff 和缺 ledger 的 closed trade 数。

补充：promotion gate 现在已经接到独立的策略激活和启用状态层。`quant.backtest.strategy_activation` 会基于 run artifact 的 `promotion_gate_report` 判断某个 run 能不能进入 `paper` 或 `live`：paper 必须满足 `paper_promotion_allowed`，live 必须满足 `production_promotion_allowed`。决策可通过 `scripts/plan_strategy_activation.py` dry-run 或 `--write` 写入 `quant.strategy_activation_decisions`，并保留 actual execution engine、allowed modes、blocked/review/missing reasons。进一步的 `quant.strategy_enable_state` 只允许从已落库且 ready/allowed 的 activation decision 写入 enabled 状态；禁用可以随时写入。这一步避免后续真实 paper/live runner 绕过 fill-first gate。

补充：真实 paper/live runner 的入口现在也有只读 guard。`quant.backtest.strategy_runner_guard` 和 `scripts/build_strategy_runner_plan.py` 只从 `quant.strategy_enable_state.enabled=true` 生成执行计划，且每个计划项必须有 ready/allowed activation decision。计划里明确订单状态回写 `quant.real_order_state_events`、校准样本进入 `quant.quant_backtest_calibration_orders`，因此后续真实下单适配器接入时不能直接绕过 fill-first 校准链路。

补充：run artifact audit 现在还包括 `Event-Level Risk Report`，用 event outcome snapshot、订单、交易和 ledger 汇总多 outcome 事件级 exposure，检查 probability sum、YES/NO complement、outcome correlation 和 portfolio cash-at-risk。它解决 FIFA 这类多 outcome 市场不能只看单条 outcome PnL 的问题；缺全事件快照或缺 correlation 时会标为 review。

补充：已新增 `quant.backtest.event_stream` typed event stream contract，把 Market/PriceBlock/Signal/Order/Fill/Ledger/Settlement 统一成按 block 或 timestamp 排序的事件；run artifact audit 会输出 `Event Stream Contract Report`。这一步为后续 multi-outcome joint replay 做基础，但当前仍保持 fill-first，不要求 LOB。

补充：已新增 `Joint Replay Plan Report`。它会把一个或多个 outcome 的 typed event stream 合并成全局 x-order 计划；单 outcome 会标记为 `single_outcome_only`，多个 outcome 同时具备 ORDER/FILL/LEDGER contract 且排序正确时才标记为 `joint_replay_ready`。这个报告解决的是“多 outcome 不能只把单 outcome 结果相加”的可审计前置条件，仍然不依赖 LOB。Strategy Tester 的 Reproducibility Snapshot 会展示 `Joint replay` verdict，并在复制 JSON 中输出完整 `joint_replay` 报告。

补充：已新增 `Joint Replay Execution Report`、持久化 `joint_event_stream` run 和原生 `native_joint_event_stream` runner。它不是只检查计划，而是直接消费合并后的 typed event stream，按全局 block/timestamp 顺序逐条更新订单、fill、ledger、cash balance、position、max cash-at-risk 和 portfolio equity。这个 primitive 仍然是 fill-first / OrderFilled-calibrated，不依赖 LOB；API 已暴露 `/quant/backtest-joint-replay` 和 `/quant/backtest-joint-runs`，后端 `build_native_joint_threshold_outcomes` 可以在一个任务里对多 outcome 按全局 block 顺序发单、成交、结算并写入 run/orders/trades/ledger/equity/metrics。前端 Batch Top-N 会直接提交 `nativeJointRun: true` 生成一个可查询、可审计的 native joint run，并显示 `Joint execution` verdict、joint run id、joint max risk 和 portfolio equity，避免 batch 页面只展示逐 outcome 指标。后续重点是接真实 shadow/live fill evidence 校准 fill probability、no-fill、slippage 和 latency。

补充：`Execution Regime Report` 现在按 market category、time-to-expiry、volatility、liquidity、maker/taker、final-minute、event/outcome count、no-fill reason 拆分订单生命周期；Strategy Tester 的 Regime 页会显示当前 run 的各桶 submitted/filled/no-fill/fill rate，避免 5m、体育盘、多 outcome 长周期市场被混在一个平均结果里。

补充：run artifact audit 现在还包括 `Materialized Replay Cache Report`，检查回测主价格输入是否来自 `quant.market_token_block_close`/block replay 这类材料化快路径，`access_path` 是否体现 token/market + block range，raw `orderfilled_fact` 是否只作为限定 market/token/window 的成交明细证据，以及本次 run 是否保存了 input snapshot/cache key。Strategy Tester 会显示 cache verdict、source、access、data version 和 raw detail 是否 bounded。

补充：已新增 `quant.market_token_execution_summary`、`quant.market_execution_summary`、`quant.event_execution_summary`，它们从处理后的 `market_token_block_close` 汇总 token/market/event 级 volume、trade count、maker/taker share、异常计数、side/liquidity bucket 和 block coverage。这个 summary 用于筛选/top-N/执行质量诊断，不改变价格线，也不会把原始 raw close 重新当 fair price。Quant 前端搜索会合并 market/event summary 候选，Strategy Tester 的 Market Screener 会显示 `Materialized execution summary` 排行，避免筛选页默认依赖临时 batch run 或在线 group 明细。ClickHouse block-close 同步脚本写完受影响 token 后会默认刷新对应 token/market/event summary，减少手工刷新导致的筛选滞后。

## L2 数据优先考虑 PMXT

下一阶段 L2 数据优先考虑 PMXT，因为它更接近 prediction-market-backtesting 的路线：

```text
PMXT raw parquet
  -> 过滤 market/token/window
  -> 解析 book_snapshot / price_change
  -> 重建 L2 MBP order book
  -> 生成 sampled snapshots / deltas
  -> DEPTH fill / queue position 近似
```

PMXT 的作用不是替代 `orderfilled_fact`，而是补齐盘口状态：

- `book_snapshot` 用来建立完整盘口基线。
- `price_change` 用来更新盘口档位。
- 本地 `LocalOrderBook` 用来维护每个 token 的 L2 book。
- 回测时用 L2 book 判断深度、spread、slippage、stale。
- raw OrderFilled 继续作为 trade evidence，用于确认成交和校准 queue。

## 推荐开发顺序

1. 固化 raw OrderFilled replay 报告：
   `raw event count`、`raw/fallback`、`loaded block window`、`candidate events`、`no_fill_reason`、`latency submit block`。

   当前已新增真实订单状态事件导入路径：

   ```bash
   conda run -n polyBacktest python scripts/import_real_order_state_events.py \
     --input runtime_outputs/order_state_events.jsonl \
     --source clob-api \
     --run-id <run_id>
   ```

   导入后，`execute_backtest_run()` 会读取同一 `run_id` 的 `quant.real_order_state_events`，按 `order_id` 或 `external_order_id` 自动附加到订单 `meta.real_order_state_events`，再进入 Fill Quality。这个阶段解决的是 fill-first 回测里的真实 submit/accepted/cancel/status 证据，不依赖 LOB。

2. PMXT parquet 读取验证：
   确认能筛出指定 market/token/window 的 `book_snapshot` 和 `price_change`。

   当前已新增只读验证脚本：

   ```bash
   conda run -n polyBots python scripts/validate_pmxt_l2_raw.py \
     --pmxt-root /path/to/pmxt/raw/mirror \
     --condition-id <condition_id> \
     --token-id <token_id> \
     --start-hour 2026-03-21T12 \
     --end-hour 2026-03-21T12
   ```

   它支持两种 PMXT raw parquet 形态：

   - payload schema：`market_id / update_type / data`
   - fixed schema：`timestamp / market / event_type / asset_id / bids / asks / price / size / side`

   验证逻辑是：筛出目标 market/token 后，把 `book_snapshot` 或 `book` 当作盘口基线，把 `price_change` 当作档位更新，喂给本项目已有的 `LocalOrderBook`，最后输出 `best_bid`、`best_ask`、`mid`、`spread`、depth、样例事件和是否 ready。

   本机当前没有搜索到真实 PMXT raw mirror，所以还不能对真实 World Cup market 做 L2 对齐验证；脚本已用 synthetic PMXT parquet 跑通两种 schema。也就是说，当前阻塞点不是解析代码，而是需要先下载或指定本地 PMXT raw parquet 路径。

   已补充下载一小时 PMXT 真实样例验证：

   - 文件：`polymarket_orderbook_2026-06-22T07.parquet`，约 251MB。
   - market：`Will France win the 2026 FIFA World Cup`。
   - condition id：`0x9b6fef249040fd17e9c107955b37ac2c3e923509b6b0ff01cc463a331ddeb894`。
   - YES token：`108233603819467706476318984012158651931658302669301887462181073562758483842092`。
   - PMXT fixed schema 可以读取，样例中 `book/price_change` 可转成 L2 book。
   - 5000 条匹配事件验证结果：`status=ready`，`best_bid=0.197`，`best_ask=0.198`，`mid=0.1975`。
   - 同一 UTC 小时内，`market_token_block_close` 里 France YES 的 `close_price` 约为 `0.1970` 到 `0.1977`，说明 PMXT L2 与当前 `orderfilled_block_close` 可以通过 `block_timestamp` 做时间对齐。

   注意：PMXT raw 行顺序不保证严格按 timestamp 单调递增，验证器需要先按事件时间排序再 replay；否则本地 book 会被标记为 `out_of_order/stale`。

3. 建 L2 本地状态机：
   用 snapshot 初始化，用 price_change 更新，遇到 gap/stale/reset 时标记不可用。

4. 生成可回测快照：
   将 L2 book 压缩成 sampled snapshots，落到本地物化表。

   当前已新增 PMXT L2 物化脚本：

   ```bash
   conda run -n polyBots python scripts/materialize_pmxt_l2_snapshots.py \
     --pmxt-root runtime_outputs/pmxt_l2_validation/raw_sample \
     --condition-id 0x9b6fef249040fd17e9c107955b37ac2c3e923509b6b0ff01cc463a331ddeb894 \
     --token-id 108233603819467706476318984012158651931658302669301887462181073562758483842092 \
     --token-side YES \
     --start-hour 2026-06-22T07 \
     --end-hour 2026-06-22T07 \
     --sample-interval-seconds 60 \
     --depth-levels 25 \
     --write \
     --replace
   ```

   写入目标是 `quant.clob_orderbook_snapshots`，`source='pmxt_l2_sampled'`。脚本默认 dry-run，只有加 `--write` 才写库；加 `--replace` 会删除同 token/source/时间窗口旧快照再插入，避免重复堆积。

   France YES 的一小时样例已写入 58 条 1 分钟 L2 sampled snapshots：

   - 时间：`2026-06-22 07:02:31 UTC` 到 `2026-06-22 07:59:00 UTC`
   - 最终盘口：`best_bid=0.197`，`best_ask=0.198`，`mid=0.1975`
   - builtin `load_clob_execution_snapshots()` 可以解析这些 PMXT 快照
   - DEPTH smoke：`2026-06-22 07:31:58 UTC` 买入 100 YES，使用 `07:31:02 UTC` PMXT snapshot，staleness 约 56 秒，以 `0.198` 完全成交

   当前也已新增 event 批量物化脚本，避免 top outcomes 反复扫描同一个 250MB parquet：

   ```bash
   conda run -n polyBots python scripts/materialize_pmxt_l2_event.py \
     --pmxt-root runtime_outputs/pmxt_l2_validation/raw_sample \
     --event-slug 2026-fifa-world-cup-winner-595 \
     --top-n 5 \
     --token-side YES \
     --start-hour 2026-06-22T07 \
     --end-hour 2026-06-22T07 \
     --sample-interval-seconds 60 \
     --depth-levels 25 \
     --write \
     --replace
   ```

   这个脚本一次扫描 PMXT 小时文件，批量筛出多个 condition/token，然后分别 replay 到各自的 `LocalOrderBook`。

   World Cup top 5 的 `2026-06-22T07` 样例已写入 288 条 1 分钟 L2 snapshots：

   | Outcome | Snapshots | Last bid/ask |
   | --- | ---: | --- |
   | Spain | 60 | 0.138 / 0.139 |
   | England | 57 | 0.125 / 0.126 |
   | France | 58 | 0.197 / 0.198 |
   | Brazil | 53 | 0.057 / 0.058 |
   | Argentina | 60 | 0.119 / 0.120 |

   回测的 `execution_depth` context 也已补充来源字段：

   ```json
   {
     "source_counts": {"clob-book": 2, "pmxt_l2_sampled": 58},
     "pmxt_l2_sampled_count": 58,
     "uses_pmxt_l2_sampled": true,
     "latest_source": "pmxt_l2_sampled"
   }
   ```

5. 接入 DEPTH 模式：
   用 L2 depth 模拟市价/限价成交，raw OrderFilled 用于校准 fill 和 queue。

## 判断标准

短期可用标准：

- 每次回测都能明确说明是否使用 raw OrderFilled。
- 没有 raw 明细时必须显示 fallback，不允许伪装成严格回测。
- no fill 必须有结构化原因。

中期可用标准：

- PMXT L2 能按 market/token/window 重建盘口。
- DEPTH 模式能报告 no-book、stale-book、depth not enough。
- 回测结果同时包含价格信号、raw 成交证据、L2 盘口质量。

最终目标：

```text
价格线用于信号
raw OrderFilled 用于成交证据
PMXT L2 用于盘口深度和队列近似
ledger 用于最终 PnL
```

## paper/live intent 层

当前 fill-first 主线已经补了从 backtest 到 paper/live 的安全入口：

- `quant.strategy_activation_decisions`：保存某次 run 是否允许进入 paper/live。
- `quant.strategy_enable_state`：保存当前真正启用的 strategy/version/mode/market/outcome。
- `strategy_runner_guard`：只从 `enabled=true` 的启用状态生成执行计划，并声明真实订单状态必须写入 `quant.real_order_state_events`。
- `guarded_executor`：把执行计划转成 submit intent。默认 dry-run，不提交真实订单；只有显式 record intent 才写入 `real_order_state_events`。
- `order_execution_adapter`：把 submit intent 转成外部 paper/live 订单 API request；默认 dry-run 只展示 request template，只有显式 `--execute` 且配置 submit URL 才调用外部接口。接口回执会标准化回 `real_order_state_events`。
- `order_execution_safety`：审计 `ORDER_EXECUTION_*` 配置，并在运行时阻断危险执行。`--execute` 必须同时 `--record-events`；非本地 endpoint 必须有 auth header；live 执行必须显式确认 `ORDER_EXECUTION_LIVE_CONFIRM=I_UNDERSTAND_LIVE_ORDER_RISK`。
- `order_execution_calibration`：adapter 回执写入后可直接构造 fill calibration samples，进入 `quant.quant_backtest_calibration_orders`，避免真实 submit/fill 回执只停在订单状态表里。
- `fill_first_production_readiness`：把真实执行 endpoint、成本源、incident 源、external signal 源、order-state collection 和 live 确认合并成一个启动前 preflight，输出 `launch_allowed` 和阻断原因。

这一步的意义是把“研究回测结果”和“准备真实执行”分开：没有外部订单适配器时，系统只能记录 intent，不能假装已经完成真实 paper/live 成交。后续真实 submit、cancel、fill 回执仍然要进入同一张订单状态证据表，再反向校准 fill model。

本地 dry-run：

```bash
conda run -n polyBacktest python scripts/run_order_execution_adapter.py \
  --target-mode paper \
  --format json
```

执行前审计配置：

```bash
conda run -n polyBacktest python scripts/audit_order_execution_env.py \
  --env-file /etc/prediction-market-quant/real-order-state-collector.env
```

启动前总 preflight：

```bash
conda run -n polyBacktest python scripts/check_fill_first_production_readiness.py \
  --target-mode paper \
  --env-file /etc/prediction-market-quant/real-order-state-collector.env \
  --env-file /etc/prediction-market-quant/fill-first-external-sources.env \
  --check-db \
  --max-stale-seconds 86400
```

真实 paper/live endpoint 接入时才使用：

```bash
ORDER_EXECUTION_SUBMIT_URL=https://replace-with-private-paper-api.example/orders \
ORDER_EXECUTION_AUTH_HEADER='Authorization=REPLACE_WITH_PRIVATE_AUTH_VALUE' \
conda run -n polyBacktest python scripts/run_order_execution_adapter.py \
  --target-mode paper \
  --execute \
  --record-events \
  --build-calibration
```
