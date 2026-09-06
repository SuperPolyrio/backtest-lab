# NautilusTrader 回测框架技术分析

源仓库：

`/home/jiahuaiyu/develop/polymarket/githubProjects/nautilus_trader`

## 1. 框架定位

NautilusTrader 是一个通用交易系统框架，不是只服务 Polymarket 的回测工具。它的设计目标是把研究、回测、模拟交易、实盘交易放在同一套执行语义下面。

核心特点：

- 多资产、多交易所、多策略。
- 事件驱动。
- research/backtest/live 尽量共享同一套执行语义。
- 支持订单生命周期、撮合、手续费、延迟、持仓、账户、风控、性能分析。

它更像一个完整的交易系统底座，而不是一个单纯的 pandas 回测脚本。

## 2. 用的什么语言

NautilusTrader 是混合语言架构：

- Rust：核心交易引擎方向，性能敏感逻辑、撮合、执行模型、基础数据类型逐步 Rust-native。
- Python：策略编写、配置、研究、回测入口。
- Cython：大量历史核心模块仍使用 `.pyx`，例如 backtest engine、portfolio、accounting。
- PyO3：Rust 和 Python 之间的绑定桥。

典型代码位置：

- `nautilus_trader/backtest/engine.pyx`
- `nautilus_trader/portfolio/portfolio.pyx`
- `nautilus_trader/analysis/analyzer.py`
- `crates/execution`
- `crates/model`

这说明它不是纯 Python 回测框架。Python 更多是策略和编排层，核心执行路径尽量下沉到 Cython/Rust。

## 3. 用的什么历史价格数据

NautilusTrader 本身不绑定单一价格源。它定义统一的数据模型，让不同来源的数据先转成标准事件，然后交给回测引擎。

支持的历史数据类型包括：

- `QuoteTick`
- `TradeTick`
- `Bar`
- `OrderBookDeltas`
- `OrderBook`
- custom data

README 中明确提到，回测可以使用 quote tick、trade tick、bar、order book、custom data，并支持纳秒级时间戳。

常见数据来源包括：

- Parquet catalog。
- CSV/自定义 loader。
- Databento。
- Tardis。
- Binance 等交易所 adapter。
- Polymarket adapter。

因此 Nautilus 的数据路线是：

```text
原始数据源 -> adapter/loader -> Nautilus 标准数据对象 -> BacktestEngine
```

对 Polymarket 来说，真正重要的不是简单的一条 close price，而是能否提供：

- 成交 tick。
-盘口快照。
-盘口增量。
-交易费用。
-合约到期/结算规则。

## 4. 怎么判断历史买入和卖出

Nautilus 把“产生买卖信号”和“历史能否成交”拆开。

第一层：策略判断买卖。

策略在事件回调里读取历史数据，比如：

- `on_bar`
- `on_trade_tick`
- `on_order_book_deltas`
- `on_order_book`

策略根据自己的规则生成订单，例如：

- 均线交叉买入/卖出。
- 突破买入/卖出。
- order book imbalance 买入/卖出。
- 价差套利。

第二层：撮合引擎判断订单是否成交。

订单提交后不会直接按 close 价成交，而是进入模拟交易所：

- 市价单根据盘口/成交数据成交。
- 限价单根据价格、队列、历史成交、盘口变化判断是否成交。
- 可以配置 fill model。
- 可以配置 fee model。
- 可以配置 latency model。
- 可以配置 liquidity consumption。
- 可以配置 queue position。

回测流程可以理解为：

```text
历史事件流
  -> 策略回调
  -> 策略提交订单
  -> BacktestEngine / MatchingEngine 撮合
  -> 生成成交事件
  -> 更新账户、持仓、PnL
```

这个设计比“价格穿过限价就直接成交”更严格。

## 5. 用什么收益指标测试策略

NautilusTrader 自带 PortfolioAnalyzer。回测结束后，`BacktestEngine.get_result()` 会返回：

- `stats_pnls`
- `stats_returns`
- `total_events`
- `total_orders`
- `total_positions`
- `elapsed_time`

已注册的收益统计包括：

- total PnL。
- total PnL percentage。
- returns volatility。
- average return。
- average win。
- average loss。
- Sharpe Ratio。
- Sortino Ratio。

tearsheet/reporting 还支持：

- equity curve。
- drawdown。
- monthly returns。
- yearly returns。
- returns distribution。
- rolling Sharpe。
- benchmark comparison。

它的收益分析不是只看最终收益，而是有完整的组合级表现分析。

## 6. 怎么保证回测快

NautilusTrader 的性能路线主要是工程架构层面的：

- 核心逻辑使用 Rust/Cython。
- 事件驱动，避免高层 Python 热循环成为瓶颈。
- `Price`、`Quantity`、`Money` 使用整数精度表示，减少浮点误差。
- 使用高性能缓存和标准化数据对象。
- 支持多 venue、多 instrument、多 strategy 在同一个引擎里运行。

项目还有专门的 `BENCHMARKING.md`，明确性能测试方法：

- Criterion：测试 wall-clock 时间。
- iai / Cachegrind：测试 CPU 指令数，用于稳定比较。
- CodSpeed：跑 Python performance tests。
- flamegraph：定位热点路径。

性能敏感路径包括：

- matching core。
- message bus。
- order cache。
- data/event 处理。

它的理念是：没有 benchmark 数据，不应该声称某个改动“更快”。

## 7. 怎么保证回测准

NautilusTrader 的准确性来自几个层面：

- 使用 deterministic time model。
- backtest 和 live 尽量共享执行语义。
- 使用纳秒时间戳。
- 订单生命周期完整，包括 accepted、filled、cancelled、rejected 等状态。
- 支持真实的 fee model。
- 支持 latency model。
- 支持 fill model。
- 支持 order book replay。
- 支持 liquidity consumption。
- 支持 queue position。
- 使用整数精度的 `Price`、`Quantity`、`Money`，避免浮点累计误差。

它的准确性重点不是“价格线画得像”，而是“策略提交的订单在当时历史市场状态下是否真的可能成交”。

## 8. 对自研 builtin 引擎的启发

NautilusTrader 值得参考的不是所有复杂功能都照搬，而是几个底层原则：

1. 数据事件和订单事件必须分开。
2. 策略信号和成交模拟必须分开。
3. PnL 必须来自成交和持仓，不应该直接来自价格线。
4. 价格、数量、金额最好避免浮点随意计算。
5. 回测输出要包含 requested data、actual loaded data、coverage、fills、fees、slippage。
6. 快不只是算法快，更重要是缓存、列裁剪、窗口裁剪、事件结构设计。

如果你的 builtin 引擎继续走自研路线，短期不需要做到 NautilusTrader 那么通用，但必须先补齐：

- 订单生命周期。
- 成交模型。
- 手续费模型。
- 持仓/PnL 账本。
- 数据覆盖率和可成交性校验。
- 回测指标体系。
