# polymarket-toolkit 回测框架技术分析

源项目：

`/home/jiahuaiyu/develop/polymarket/githubProjects/polymarket-toolkit`

结论先说：`polymarket-toolkit` 不是严格意义上的回测框架。它更像一个 Polymarket 公开 API 工具箱，重点是账户画像、PnL 重建、Brier Score、Gamma/Data/LB/CLOB 读取，以及 redeem 状态监控。它没有完整的历史事件循环、策略下单、订单生命周期、fill/no-fill、盘口 replay、滑点和队列模拟。

但是它有一个很值得参考的部分：用 Data API 的真实活动流水重建账户级 PnL。这个可以作为我们自研 builtin 引擎后面的 ledger / PnL 校验层参考，而不是作为执行撮合层参考。

## 1. 框架定位

它的定位不是“输入历史行情 -> 跑策略 -> 模拟成交 -> 输出回测报告”。

它主要做的是：

- 读取 Polymarket 公开 API：Gamma、Data API、LB API、CLOB REST、CLOB WebSocket。
- 对地址做画像：PnL、胜率、持仓、类别、交易风格。
- 用活动流水重建更准确的 PnL。
- 用 settled positions 计算 Brier Score。
- 读取当前 order book、midpoint、实时 WebSocket 盘口。
- 只读扫描 redeemable positions。

所以它解决的是“已经发生了什么”和“一个账户真实赚亏多少”，不是“如果我的策略在历史上这样下单，会不会成交，会赚多少钱”。

## 2. 用的什么语言

项目是混合语言：

- TypeScript：`src/index.ts` 和 `examples/*.ts`，负责公开 API helper、Gamma/Data/LB/CLOB 请求、order book/midpoint/WebSocket 示例、Brier 简化函数。
- Python：`skills/polymarket-pnl/compute_precise_pnl.py`，负责审计级 PnL 重建、分页、checkpoint、benchmark。

它没有 Rust 撮合核心，也没有专门的高性能回测 engine。

## 3. 用的什么历史价格数据

严格讲，它没有使用“历史价格数据”来做回测。

它读取的数据主要分几类：

| 数据 | 来源 | 用途 |
| --- | --- | --- |
| 市场/事件元数据 | `gamma-api.polymarket.com` | 找 market、event、token id |
| 地址活动流水 | `data-api.polymarket.com/activity` | BUY / SELL / REDEEM / MERGE / SPLIT / REBATE 等现金流重建 |
| 当前持仓 | `data-api.polymarket.com/positions` | 当前未实现价值、settled position、Brier Score |
| 排行榜 PnL | `data-api.polymarket.com/v1/leaderboard`、`lb-api.polymarket.com/profit` | 官方口径对照、benchmark |
| 当前 CLOB book | `clob.polymarket.com/book` | 当前盘口快照 |
| 当前 midpoint | `clob.polymarket.com/midpoint` | 当前中间价 |
| 实时 CLOB WebSocket | `ws-subscriptions-clob.polymarket.com/ws/market` | 实时 `book` / `best_bid_ask` / `price_change` |

注意：它的 CLOB 数据是当前/实时读取，不是历史 L2/L3 数据 replay。也没有 PMXT/Telonex 这种历史 LOB parquet 的读取、落库、对齐、回放。

因此它不能回答“历史某个时间点盘口深度是多少、我的订单能不能以这个价格成交、排队位置如何变化”。

## 4. 怎么判断历史买入和卖出

它不是判断“策略应该买入/卖出”，而是读取 Data API 已经发生的活动。

在 `compute_precise_pnl.py` 里，核心逻辑是：

- `type=TRADE` 的活动里，按 `side == BUY` 统计买入现金流。
- `side == SELL` 统计卖出现金流。
- 另外单独读取 `REDEEM`、`MERGE`、`SPLIT`、`MAKER_REBATE`、`REWARD`、`REFERRAL_REWARD`、`CONVERSION`。
- 对 open positions 用 `size * curPrice` 计算未实现价值。

这套方法判断的是“真实账户历史上发生过哪些交易和资金流动”，不是“模拟订单是否成交”。

它没有：

- strategy signal。
- submit order。
- maker/taker 判定。
- fill/no-fill。
- partial fill。
- cancel / replace。
- latency submit block。
- 价格击穿。
- order book queue。

## 5. 用什么收益指标测试策略

它没有完整策略回测收益指标体系。

它的收益/质量指标主要是：

- `pnl`：交易现金流 PnL。
- `pnl_inclusive`：交易 PnL 加上 REWARD / REFERRAL_REWARD / CONVERSION。
- `total_buy`、`total_sell`、`total_redeem`、`total_merge`、`total_split`、`total_rebate`。
- `unrealized`：当前持仓未实现价值。
- `open_positions`。
- `trade_count`。
- `complete`、`pagination_incomplete`、`positions_truncated`：数据完整性标记。
- `MAPE`：和官方 leaderboard `/profit` 对照的误差。
- Brier Score：用 settled positions 的 `avgPrice` 当预测概率，用 `currentValue > 0` 判断是否押中。

这更偏“账户审计”和“预测质量评分”，不是策略回测报告。

它没有我们回测框架应该有的：

- equity curve。
- drawdown。
- Sharpe / Sortino。
- turnover。
- win/loss by trade。
- fill quality。
- slippage。
- no_fill_reason。
- latency cost。
- market coverage。
- signal coverage。

## 6. 怎么保证快

它的快主要来自简单直接，而不是高性能回测架构：

- TypeScript helper 是零依赖的 HTTP/WebSocket 包装。
- Python PnL 脚本按 API 分页拉取。
- 有 `REQUEST_DELAY = 0.2` 控制请求节奏。
- 有 checkpoint，可以恢复长任务。
- 输出 JSONL，边算边写。
- leaderboard / address list 可以批量处理。

但它没有：

- 本地历史行情缓存。
- parquet/duckdb 扫描优化。
- 物化表。
- event-driven engine。
- Rust/C++ 撮合。
- 多市场历史 replay 调度。

所以它适合 API 级分析，不适合直接承载大规模历史回测。

## 7. 怎么保证准

它最值得学习的是 PnL 准确性设计。

核心思路是不要直接相信 position-level `cashPnL`，而是用现金流恒等式重建：

```text
PnL = SUM(SELL + REDEEM + MERGE + REBATE)
      - SUM(BUY + SPLIT)
      + unrealized_position_value
```

然后另算一个 inclusive 口径：

```text
pnl_inclusive = pnl + REWARD + REFERRAL_REWARD + CONVERSION
```

它还处理了几个容易出错的 API 边界：

- TRADE 活动用 timestamp 分页，避免 offset cap。
- full page 落在同一秒边界时，尝试 exact-second replay，避免漏掉同秒成交。
- 不去重同秒、同交易、同金额的成交，因为 maker fill 可能真的产生多条相同现金流记录。
- positions 分页达到 API cap 时打 `positions_truncated`。
- 活动分页不完整时打 `pagination_incomplete`。
- 和官方 leaderboard PnL 做 benchmark，统计 MAPE。

这部分对我们的启发很明确：回测结果最终也应该落到现金流 ledger，而不是只看价格线收益。

## 8. 它和严格回测框架的差距

如果和 `prediction-market-backtesting` 或 NautilusTrader 这类严格回测框架比，`polymarket-toolkit` 缺少的是执行层。

关键缺口：

| 模块 | polymarket-toolkit | 严格回测需要 |
| --- | --- | --- |
| 历史行情 | 活动流水、当前 CLOB、当前/实时 midpoint | 可 replay 的历史价格 / trade / L2 book |
| 策略循环 | 没有 | 按时间或 block 推进策略 |
| 下单模型 | 没有 | order submit / cancel / replace |
| 成交模型 | 只读取真实历史 TRADE | 模拟 fill/no-fill/partial fill |
| LOB | 当前 REST/WS | 历史 L2/L3 replay、depth、queue |
| 延迟 | 没有 | signal time -> submit time -> fill time |
| 滑点 | 没有 | 根据盘口或成交事实计算 |
| 结果报告 | PnL/Brier/leaderboard benchmark | equity/drawdown/fill quality/coverage/no-fill reason |

所以它不能替代我们的 builtin 引擎。

## 9. 对自研 builtin 引擎的启发

它应该被放在“账户流水和 PnL 校验参考”的位置。

对我们有价值的点：

1. ledger 口径要完整

不能只记录 BUY/SELL。预测市场还要考虑：

- REDEEM
- MERGE
- SPLIT
- MAKER_REBATE
- REWARD
- REFERRAL_REWARD
- CONVERSION
- open position unrealized value

2. PnL 不要只依赖 position snapshot

position-level `cashPnL` 会丢掉 partial fills、四舍五入和复杂现金流。我们的回测输出也应该从 simulated fills 生成 ledger，再由 ledger 算 PnL。

3. 数据完整性要显式暴露

它的 `complete`、`pagination_incomplete`、`positions_truncated` 这类字段很实用。我们自己的报告里也应该有：

- loaded block window
- raw/fallback
- candidate events
- no_fill_reason
- fill evidence count
- missing book / missing fill
- latency submit block

4. 真实成交流水可以用来校准回测

它读取真实 TRADE / REDEEM / MERGE / SPLIT 的方式，可以帮助我们校验：

- simulated fill 是否接近真实 fill。
- simulated PnL 是否能被现金流解释。
- maker/taker、rebate、split/merge 是否漏算。

5. Brier Score 可以作为策略质量的辅助指标

Brier 不等于收益，但它能区分“预测能力”和“赚钱能力”。对预测市场策略来说，可以作为报告里的辅助维度：

- 信号概率是否校准。
- 高置信度下注是否真的更容易赢。
- PnL 是来自预测能力，还是来自 spread、rebate、套利。

## 10. 对当前项目的实际建议

不要把 `polymarket-toolkit` 当作回测引擎抄。

更合理的吸收方式是：

1. builtin 引擎继续自己做执行模拟

核心还是：

- `orderfilled_block_close` / raw fill evidence
- price breakthrough / fill-based execution
- PMXT/Telonex L2 depth execution
- latency
- no-fill
- partial fill
- order lifecycle

2. 从它学习 ledger 和 PnL

我们的回测结果应该从 fill 生成标准现金流：

- BUY：现金流出
- SELL：现金流入
- REDEEM：结算流入
- MERGE：合并返还
- SPLIT：拆分成本
- REBATE：maker rebate
- unrealized：持仓市值

3. 把完整性字段放进报告

只要数据缺失、分页不完整、历史盘口缺失、fill 证据不足，就应该明确展示，不要让一个 PnL 数字看起来过度确定。

4. Brier 可以后置

先把成交和 PnL 做准，再加入 Brier / calibration。否则策略评分会被错误成交模型污染。

最终判断：

`polymarket-toolkit` 对我们的价值不是“怎么回测”，而是“回测成交之后，怎么把真实/模拟流水算成可信 PnL，并且怎么做账户级质量校验”。
