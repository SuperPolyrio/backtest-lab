# prediction-market-backtesting 回测框架技术分析

源仓库：

`/home/jiahuaiyu/develop/polymarket/githubProjects/prediction-market-backtesting`

## 1. 框架定位

`prediction-market-backtesting` 是一个面向预测市场，尤其是 Polymarket 的回测研究框架。

它不是完全从零自研撮合内核，而是在 NautilusTrader 上面增加了：

- Polymarket 数据加载。
- PMXT/Telonex 数据适配。
- prediction market instrument/fee/settlement 处理。
- 策略样例。
- 批量 runner。
- 参数搜索。
- 研究报告。
- 多市场组合分析。

可以理解为：

```text
prediction-market-backtesting
  = Polymarket 数据层
  + 策略研究层
  + 报告分析层
  + NautilusTrader 回测/撮合/账户内核
```

所以它更接近“预测市场量化研究框架”，不是单独替代 Nautilus 的回测引擎。

## 2. 用的什么语言

主要语言和组件：

- Python：策略、runner、配置、研究脚本、报告。
- Rust：数据转换和加速层。
- NautilusTrader：底层 backtest engine、撮合、账户、订单、持仓、PnL。
- Pandas / Arrow / Parquet / DuckDB：数据读取、缓存、转换、报告。

典型代码位置：

- `prediction_market_extensions/backtesting/_prediction_market_backtest.py`
- `prediction_market_extensions/backtesting/_execution_config.py`
- `prediction_market_extensions/backtesting/data_sources`
- `prediction_market_extensions/adapters/polymarket`
- `strategies`
- `backtests`

README 中写到 Python 3.12+、Rust、NautilusTrader 1.226.0，并强调 V4 有 Rust-native data conversion。

## 3. 用的什么历史价格数据

这个框架的公开 Polymarket runner 主要使用 L2 market-by-price order book replay。

核心历史数据来源：

- PMXT Polymarket order book archive。
- Telonex `book_snapshot_full`。
- Polymarket public trades。
- Telonex `onchain_fills`。
- Telonex `trades`。
- 本地 mirror。
- archive。
- API fallback。

关键数据对象：

- `OrderBookDeltas`：更新 L2 order book。
- `TradeTick`：作为历史成交证据，给撮合和 queue position 使用。

文档 `docs/execution-modeling.md` 明确说：

- `QuoteTick` 不是这里的 L2 replay 输入。
- `OrderBookDeltas` 负责移动 L2 order book。
- `TradeTick` 在 `trade_execution=True` 时触发 matching 和 queue-position 更新。
- 公开 runner 中，策略不应该订阅 trade tick 做信号，trade prints 是执行证据，不是策略数据源。

PMXT 路线：

- 原始 hourly Polymarket order-book archives。
- loader 过滤 market/token。
- 解码 `book_snapshot` 和 `price_change`。
- 生成 Nautilus `OrderBookDeltas`。
- 如果某小时缺失，会 reset 本地 book state，直到新 snapshot 重建盘口。

Telonex 路线：

- 使用 `book_snapshot_full`，不是 shallow snapshot。
- full-depth snapshots diff 成 `OrderBookDeltas`。
- 真实 Polymarket trade ticks 和 Telonex book deltas 交错进入回测。
- 转换后写入 materialized cache：`book-deltas-v1`。

## 4. 怎么判断历史买入和卖出

这个框架也把“策略信号”和“成交模拟”分开。

### 4.1 策略怎么产生买卖信号

策略在 `strategies/` 目录里，典型策略包括：

- `microprice_imbalance`
- `late_favorite_limit_hold`
- `binary_pair_arbitrage`
- `breakout`
- `panic_fade`
- `deep_value`
- `threshold_momentum`
- `book EMA crossover`

以 `BookMicropriceImbalanceStrategy` 为例：

1. 订阅 L2 `OrderBookDeltas`。
2. 维护本地 `OrderBook`。
3. 读取 best bid、best ask、spread、midpoint。
4. 统计 bid depth 和 ask depth。
5. 计算 imbalance。
6. 计算 microprice edge。
7. 如果满足入场条件，则提交 BUY。
8. 持仓后，如果压力减弱、达到止盈/止损、满足退出条件，则提交 SELL。

入场条件包括：

- ask > bid。
- spread 小于 `max_spread`。
- imbalance 大于 `entry_imbalance`。
- microprice edge 大于 `min_microprice_edge`。
- entry price 小于 `max_entry_price`。
- expected slippage 不超过阈值。

退出条件包括：

- take profit。
- stop loss。
- imbalance 低于 `exit_imbalance`。
- microprice edge 反向。
- 最小持仓更新数/时间限制。

### 4.2 历史订单怎么判断是否成交

订单不是信号一出现就成交，而是交给 NautilusTrader 回测交易所。

公开 Polymarket book runner 的执行模型：

```python
engine.add_venue(
    book_type=BookType.L2_MBP,
    liquidity_consumption=True,
    queue_position=execution.queue_position,
    bar_execution=False,
    trade_execution=True,
)
```

含义：

- `OrderBookDeltas` 只负责更新盘口。
- `TradeTick` 可以触发 resting order 成交。
- bars 不驱动成交。
- marketable order 可以吃多档盘口。
- fills 会消耗可见盘口流动性。
- limit order 可以使用 queue position 估算排队。

被动限价单的队列逻辑：

- 提交限价单时，记录同侧同价位已有显示数量。
- 这个数量视为排在你前面的队列。
- 后续该价位 trade tick 会减少前方队列。
- 前方队列清掉后，超出的成交量才可能轮到你的订单。

这是 L2 MBP 下的近似，不是 L3 MBO 的真实 FIFO。

## 5. 用什么收益指标测试策略

这个框架继承 Nautilus 的 portfolio stats，同时自己扩展预测市场研究报告。

README 和 docs 中提到的报告内容包括：

- total equity。
- individual market equity。
- profit/loss ticks。
- P&L periodic bars。
- market allocation。
- YES price with buy/sell fills。
- drawdown。
- rolling Sharpe。
- cash/equity。
- monthly returns。
- cumulative Brier advantage。

研究优化文档 `docs/research.md` 给了明确的评分函数：

```text
score = pnl - 0.5 * max_drawdown_currency - penalties
```

penalties 会惩罚：

- early termination。
- coverage 不足。
- fill 数太少。
- trial 失败。

joint-portfolio 模式：

- 多市场 PnL 求和。
- 多市场 fill count 求和。
- drawdown 在合并后的 equity curve 上计算。
- requested coverage ratio 跨市场平均。

这个设计说明它关注的不是单次交易胜负，而是组合级收益、回撤、覆盖率和可执行性。

## 6. 怎么保证回测快

这个框架的性能优化主要在数据层。

已实现/文档提到的性能手段：

- Rust-native data conversion。
- staged data loading。
- 多 replay 异步加载。
- `BACKTEST_REPLAY_LOAD_WORKERS` 控制加载并发。
- PMXT filtered cache。
- PMXT materialized deltas cache。
- Telonex `api-days` cache。
- Telonex `book-deltas-v1` cache。
- Telonex `trade-ticks-v1` cache。
- Telonex `.fast.parquet` sidecar。
- 本地 mirror 优先，archive/API 兜底。
- DuckDB manifest 选择需要的 parquet parts。
- 只读取请求 market/outcome/date range 对应的数据。

它的快主要不是策略计算多复杂，而是避免每次重新扫描和重新转换大规模 order book 原始数据。

## 7. 怎么保证回测准

准确性设计主要来自以下几点：

- 使用 L2 order book，而不是只用 close price。
- `OrderBookDeltas` 更新盘口状态。
- `TradeTick` 作为历史成交证据。
- 支持 queue position。
- 支持 latency。
- 支持 liquidity consumption。
- 支持 taker fee。
- 支持 maker rebate 近似。
- 支持 slippage。
- 区分 requested window 和 loaded window。
- 输出 coverage ratio。
- 数据缺口会 warning。
- PMXT 缺小时会 reset 本地盘口状态。
- `min_book_events` 和 `min_price_range` 过滤低质量样本。

现实边界：

- 当前公开 Polymarket runner 是 L2 MBP，不是 L3 MBO。
- 无法知道真实订单级 FIFO。
- 无法知道 hidden liquidity。
- trade tick 只能证明某价位有成交，不能完美证明你的订单排队位置。
- maker rebate 和 liquidity rewards 不能完全还原钱包/市场级日结算。

所以它的准确性是“在公开 L2 数据条件下尽量真实”，不是“完全复制交易所撮合”。

## 8. 和 NautilusTrader 的关系

`prediction-market-backtesting` 不是独立替代 NautilusTrader。

它负责：

- Polymarket 数据接入。
- PMXT/Telonex 数据转换。
- prediction market 策略。
- 研究 runner。
- optimizer。
- report/artifacts。

NautilusTrader 负责：

- BacktestEngine。
- 订单生命周期。
- 撮合。
- 持仓。
- 账户。
- PnL。
- fills report。
- positions report。

也就是说，这个框架的优势是 prediction market domain knowledge，而不是底层交易内核本身。

## 9. 对自研 builtin 引擎的启发

这个框架对你的 builtin 研究价值比 Nautilus 更直接。

建议优先吸收以下设计：

1. 策略信号只从合法历史数据来，避免未来函数。
2. trade tick/orderfilled 数据应作为成交证据，不应该直接等价为完整可成交盘口。
3. 价格展示、信号计算、成交模拟、PnL 账本必须分层。
4. 输出里必须明确 requested window、loaded window、coverage ratio。
5. 回测结果必须显示 fills、fees、slippage、cash、equity、positions。
6. 优化目标不能只用 PnL，要加入 max drawdown 和 penalties。
7. 多市场组合要用合并 equity curve 算 drawdown，不要把单市场回撤相加。
8. 数据层必须做物化缓存，否则全量 Polymarket 历史盘口会拖垮研究效率。

如果你的 builtin 暂时只使用 `orderfilled_block_close`，需要明确标注：

- 它可以用于历史价格线和初步信号研究。
- 它不能完整模拟挂单排队。
- 它不能证明某个限价单一定成交。
- 它缺少完整 L2 深度。
- 后续要想提高准确性，需要补 order book 或 trade evidence 模型。
