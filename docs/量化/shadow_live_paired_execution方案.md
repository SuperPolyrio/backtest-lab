# Shadow/Live Paired Execution 校准方案

当前结论：暂时不上实盘。

现在 builtin 模拟盘、fill-first 质量门禁和订单执行 adapter 已经有基础，但还没有达到可以直接 live 下单的程度。下一步应该先做 shadow/paper 级别的 paired execution：同一个策略信号同时生成一笔模拟订单和一笔 paper/live 外部订单，并用统一 `pair_id` 对齐，比较两边成交结果是否一致。

## 目标

这个方案解决的问题不是价格线是否好看，而是回测执行模型准不准：

```text
同一个 signal
-> builtin 模拟订单 shadow_order
-> Polymarket paper/live 订单 live_order
-> pair_id 绑定
-> 采集真实订单状态
-> 生成 calibration sample
-> 输出模拟 vs 真实执行误差
```

最终要回答：

- 模拟认为会成交，真实是否成交。
- 模拟认为 no-fill，真实是否也 no-fill。
- 模拟成交价和真实成交价差多少。
- 模拟成交量和真实成交量差多少。
- 模拟 fee、rebate、slippage、latency 和真实差多少。
- 哪些 market、maker/taker、liquidity bucket、time bucket 偏差最大。

## 非目标

当前阶段不做：

- 不上真实 live 资金。
- 不把 LOB/DEPTH 作为主线。
- 不把 fixture 当成真实订单证据。
- 不用单次 PnL 判断策略可上线。
- 不绕过 paper/live evidence gate 直接调用下单接口。

## 核心数据关系

需要形成四层关系：

| 层级 | 说明 |
| --- | --- |
| signal | 策略在某个 block/time 产生的交易意图 |
| shadow_order | builtin 模拟盘根据同一信号生成的订单和模拟 fill |
| live_order | 外部 paper/live adapter 同步提交的真实订单 |
| calibration_sample | shadow_order 与 live_order 的状态、价格、数量、成本和延迟对比 |

建议统一使用：

```text
pair_id = run_id + signal_id + shadow_order_id
```

所有后续 order state、cost event、calibration sample 都必须能追溯到这个 `pair_id` 或至少追溯到同一组 `run_id/order_id/external_order_id`。

## 需要的脚本

### 1. `scripts/run_shadow_live_paired_executor.py`

主入口。负责同一信号下同时生成 shadow order 和外部 order。

职责：

- 读取策略/runner 配置。
- 生成 builtin shadow order。
- 生成外部 submit request。
- 默认只允许 `--target-mode paper`。
- live 必须要求 `--target-mode live --execute --confirm-live`，但当前阶段先不启用。
- 写出 pair artifact，记录 `pair_id`、shadow order、submit request、external order id。

第一版只需要支持 paper dry-run 和 paper execute，不需要 live。

### 2. `scripts/build_shadow_live_pairs.py`

把 shadow order 和外部 submit response 对齐成 pair。

输出字段至少包括：

```text
pair_id
run_id
signal_id
shadow_order_id
external_order_id
market_slug
token_id
token_side
side
role
requested_price
requested_size
shadow_status
target_mode
submit_time
submit_block
```

第一版可以写 JSONL artifact；后面再考虑建表。

### 3. `scripts/collect_shadow_live_order_status.py`

按 paired orders 精确采集外部订单状态。

职责：

- 按 `external_order_id` 查询订单状态。
- 记录 submit、accepted、filled、partial、cancelled、rejected、expired。
- 标准化写入 `quant.real_order_state_events`。
- 支持轮询直到终态。
- 支持 timeout、retry、max age。

已有 `scripts/collect_real_order_state_events.py` 可以复用，但 paired 模式需要更明确地按 pair/order id 采集。

### 4. `scripts/check_shadow_live_pair_status.py`

检查 paired execution 是否完整。

输出：

- pairs 总数。
- 有 live order id 的数量。
- 已终态订单数量。
- 缺真实状态数量。
- 缺成交价/成交量/fee/rebate/latency 的数量。
- 可进入 calibration 的 pair 数。
- 不能进入 calibration 的原因列表。

这个脚本用于防止“看起来提交了订单，但没有真实回执”的假闭环。

### 5. `scripts/build_shadow_live_calibration_samples.py`

从 pair 和 `quant.real_order_state_events` 构造 calibration samples。

比较字段：

```text
shadow_status vs live_status
shadow_fill_price vs live_fill_price
shadow_fill_size vs live_fill_size
shadow_fee vs live_fee
shadow_rebate vs live_rebate
shadow_slippage vs live_slippage
shadow_latency_seconds vs live_latency_seconds
```

输出写入 `quant.quant_backtest_calibration_orders`，或 dry-run 输出 JSON/markdown。

已有 `scripts/build_backtest_calibration_samples.py` 和 `quant/backtest/calibration_samples.py` 可以复用。

### 6. `scripts/report_shadow_live_calibration.py`

生成 paired calibration 报告。

必须包含：

- status match rate。
- fill precision。
- fill recall。
- false fill：模拟成交，真实没成交。
- missed fill：模拟没成交，真实成交。
- avg price error。
- avg size error。
- avg slippage error。
- avg latency error。
- fee/rebate error。
- maker/taker 分桶。
- market 分桶。
- liquidity/time-to-expiry 分桶。
- 是否建议调整 execution profile。

已有 `scripts/generate_backtest_calibration_report.py`、`scripts/suggest_execution_profile_calibration.py`、`scripts/apply_execution_profile_calibration.py` 可以复用。

### 7. `scripts/run_shadow_live_calibration_loop.py`

把完整流程串起来，用于长期 paper 校准。

流程：

```text
generate signal
-> shadow simulate
-> paper submit
-> collect order status
-> build calibration samples
-> report
-> update calibration state
```

第一版只做 paper，不做 live。

建议参数：

```text
--target-mode paper
--max-orders
--max-notional
--poll-interval-seconds
--timeout-seconds
--dry-run
--write
--stop-on-error
```

### 8. `scripts/check_shadow_live_safety_gate.py`

安全门禁。任何真实 submit 前必须通过。

检查项：

- fill-first quality gate 无 fail。
- 目标模式是 paper。
- live 当前禁用。
- 单笔最大 notional。
- 单日最大 notional。
- market allowlist。
- token allowlist。
- 禁止 closed/resolved market。
- 禁止低流动性 market。
- 订单状态采集可用。
- emergency stop file 不存在。
- submit adapter 必须能记录 response event。

这个脚本要比执行脚本更早失败，不能等下单后才发现风险。

## 阶段路线

### Phase A：纯离线 pair 验证

不调用外部订单接口。

目标：

- 能生成 shadow order。
- 能生成 submit request template。
- 能生成 pair artifact。
- 能跑 `check_shadow_live_pair_status.py`，状态为 dry-run ready。

验收：

```bash
conda run -n polyBacktest python scripts/run_shadow_live_paired_executor.py --target-mode paper --dry-run
conda run -n polyBacktest python scripts/check_shadow_live_pair_status.py --input <pair-artifact>
```

### Phase B：paper execute 验证

调用 paper endpoint，不用真实资金。

目标：

- paper order 有 external order id。
- 能采集 paper order 状态。
- 能生成 calibration samples。
- 能输出 shadow vs paper 误差报告。

验收：

```bash
conda run -n polyBacktest python scripts/run_shadow_live_paired_executor.py --target-mode paper --execute --write
conda run -n polyBacktest python scripts/collect_shadow_live_order_status.py --from-pairs <pair-artifact> --write
conda run -n polyBacktest python scripts/build_shadow_live_calibration_samples.py --from-pairs <pair-artifact> --write
conda run -n polyBacktest python scripts/report_shadow_live_calibration.py --run-id <run_id>
```

### Phase C：长期 paper 校准

目标：

- 连续多天 paper paired execution。
- 按 market、role、liquidity、time bucket 统计误差。
- 形成 execution profile override 建议。
- 不进入 live。

验收：

- calibration samples 足够多。
- false fill / missed fill 可解释。
- latency error 稳定。
- fee/rebate/cost 字段完整。
- quality gate 无 fail。
- run-level `order_state_coverage_pct` 和 `calibration_coverage_pct` 可审计。

### Phase D：极小额 live 预备

当前不执行，只作为未来边界。

进入条件：

- paper paired execution 连续稳定。
- 安全门禁完整。
- order state collector 稳定。
- emergency stop 可用。
- allowlist 明确。
- max notional 足够小。
- 用户人工确认。

## 当前优先级

现在最应该先实现：

1. `run_shadow_live_paired_executor.py`
2. `build_shadow_live_pairs.py`
3. `check_shadow_live_pair_status.py`

先把 pair 关系和 dry-run/paper 闭环做出来，再做真实状态采集和 calibration 报告。

## 和现有质量门禁的关系

当前 `run_id=71` 的：

```text
order_state_coverage_pct = 0%
calibration_coverage_pct = 0%
samples = 0
```

原因是它只是历史回测 run，没有对应的同步 paper/live 订单状态证据。

paired execution 跑起来后，新 run 应该天然产生：

```text
shadow_order
live_order
real_order_state_event
calibration_sample
```

这样 coverage 才是有意义的，而不是事后手工补证据。

## 重要原则

模拟盘还没做好之前，不能上实盘。

当前所有实现都应该默认 paper/dry-run，live 代码即使存在，也必须被安全门禁、显式参数和人工确认三层挡住。
