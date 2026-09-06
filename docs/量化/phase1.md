# Phase 1: OrderFilled-first 执行回测 MVP

## 目标

第一阶段不是从零实现一个新回测框架，而是在当前已经实现的 `ORDERFILLED + market_token_block_close` 基础上，补齐可审计执行回测所需的关键能力。

目标是把现在“能跑 trades 的价格/成交量回测”升级成：

- 能解释每个策略信号为什么成交、部分成交或没有成交。
- 能把 signal、order、fill、trade、ledger 串起来。
- 能用 OrderFilled block 级真实成交流回放限价单成交。
- 能区分中途真实卖出 PnL 和持有到结算的 settlement PnL。
- 能区分 optimistic / realistic / conservative / stress 执行假设。
- 能让净收益从 ledger/cashflow 推导，而不是只靠 entry/exit price 差。

第一阶段不做：

- NautilusTrader 深度集成。
- Rust/C++ 重写。
- 全量历史 CLOB 采集。
- 复杂多策略/参数优化。
- 自动实盘交易。
- 完整 multi-outcome portfolio optimizer。

## 软件工程原则：低耦合开发

Phase 1 不能把所有功能继续堆进一个脚本或一个函数里。回测系统后续要支持不同策略、执行模型、账本口径和前端展示，所以必须从第一阶段开始控制耦合。

开发原则：

- 一个功能一个模块，避免把 orders、ledger、execution profile、strategy simulator 都写进同一个文件。
- `backtest_engine.py` 只做编排：读取参数、读取价格点、调用策略、写结果。
- 执行假设和 profile 放在独立模块，例如 `execution_profiles.py`。
- 订单生命周期放在独立模块，例如 `orders.py`。
- 账本/cashflow 放在独立模块，例如 `ledger.py`。
- 数据库 schema 只放在 `quant/core/schema.py`，不要在业务逻辑里散落 `CREATE TABLE`。
- API 只做读取/提交，不在 route 里实现回测业务逻辑。
- 前端只展示和提交参数，不在前端复刻核心回测逻辑。
- 每个模块都要有可单测的纯函数，避免只能通过完整回测 run 才能验证。

模块边界：

```text
quant/backtest/backtest_engine.py      # run 编排和持久化调用
quant/backtest/execution_profiles.py   # optimistic/realistic/conservative/stress
quant/backtest/orders.py               # order intent/result/lifecycle rows
quant/backtest/ledger.py               # BUY/SELL/FEE/SLIPPAGE/account equity ledger
quant/backtest/execution.py            # CLOB depth fill helper
quant/core/schema.py                   # 表结构和索引
quant/api/read_api.py                  # 结果读取
scripts/api/routes/quant.py            # HTTP route 和 camelCase mapping
```

验收要求：

- 新增能力必须有独立模块或清晰函数边界。
- 不允许把 Phase 1 的 orders/ledger/profile 全部塞进 `simulate_strategy()`。
- 新模块必须有单元测试覆盖核心纯函数。

## 数据访问硬规则

这些规则是后续开发必须遵守的命令，不是建议。任何回测、搜索、smoke、debug 查询都必须先判断自己属于哪一类数据访问。

## 环境配置硬规则

数据库、ClickHouse、API 连接信息必须从仓库根目录 `.env`、`.env.local` 或进程环境变量读取，不能在业务代码里写死生产密码。

必须遵守：

- `quant/core/db.py` 是 Phase 1 回测数据库配置入口。
- PostgreSQL 优先读取：
  - `POLYDATA_POSTGRES_HOST`
  - `POLYDATA_POSTGRES_PORT`
  - `POLYDATA_POSTGRES_USER`
  - `POLYDATA_POSTGRES_PASSWORD`
  - `POLYDATA_POSTGRES_DATABASE`
  - `POLYDATA_POSTGRES_SEARCH_PATH`
- 兼容旧别名：
  - `POLYMARKET_POSTGRES_*`
  - `POLYMARKET_PostgreSQL_*`
- ClickHouse 优先读取：
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_HTTP_URL`
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_CONTAINER`
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_DATABASE`
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_USER`
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_PASSWORD`
  - `POLYDATA_ORDERFILLED_CLICKHOUSE_READ_TABLE`

禁止：

- 不允许在代码里写死 PostgreSQL 密码。
- 不允许在代码里写死 ClickHouse 密码。
- 不允许测试、日志、API response 打印真实密码。
- 不允许为了“本地能跑”加回生产默认密码。

允许的默认值只能是非敏感连接结构，例如：

```text
host=127.0.0.1
port=45432
database=poly_data_core
user=poly_user
search_path=quant,core,oracle,ops,public
```

密码没有配置时，应显示 `password_configured=false` 或在实际连接时失败，而不是偷偷使用代码内置密码。

### 1. market/event 搜索

只能走：

```text
quant.market_event_metadata
quant.market_event_members
quant.market_price_build_market_progress
quant.market_token_metadata
```

用途：

- command palette 搜索。
- `NBA/FIFA/Trump` 等关键词召回。
- event/outcome drilldown。
- market 候选选择。
- 最近、收藏、watchlist、coverage 展示。

禁止：

- 不允许为了找候选 market 去 `quant.market_token_block_close` 做 `GROUP BY count(*)`。
- 不允许在价格明细表上做 `%keyword%` contains 搜索。
- 不允许在在线 API 里用价格明细表承担 discovery/search 职责。

如果需要 contains 搜索，必须使用搜索表或专用索引：

```text
pg_trgm GIN(lower(event_slug/event_title/market_slug/question/outcome_label))
```

### 2. 回测价格读取

只能走：

```text
quant.market_token_block_close
```

并且必须使用以下 key/range 之一：

```text
token_id + block_number range
market_slug + token_side + block_number range
market_id + token_side + block_number range
```

禁止：

- 不允许在线回测从 `market_token_block_close` 做跨市场聚合。
- 不允许在线回测从 `market_token_block_close` 按 title/category 搜索。
- 不允许在线回测为了候选发现而 group 明细价格表。

每次 backtest run 的 artifact 必须记录：

```text
source_table
access_path
index_hint
query_guard_version
market_slug
token_side
token_id
from_block / to_block
actual_first_block / actual_last_block
row_count
data_version
```

### 3. raw OrderFilled 明细校验

raw `orderfilled_fact` 只允许用于明细校验或离线构建聚合表。

在线使用时必须同时具备：

```text
market_id
token_id
block_number range
explicit limit
```

禁止：

- 不允许在线从 14 亿 raw `orderfilled_fact` 现场 group all markets。
- 不允许 API 请求触发 raw fact 全量聚合。
- 不允许用 raw fact 承担 command palette 或 market discovery。

### 4. 聚合表优先

热门 market 和常用回测输入必须提前构建聚合表：

```text
block close
block volume
trade count
side bucket
maker/taker stats
```

Python 回测层只处理几千到几十万行聚合结果，不处理十亿级 raw fact。

## 当前已经实现的基础

当前代码已经不是空白状态，已有这些能力：

- `ORDERFILLED` 已经是默认 `execution_price_mode`。
- `fetch_price_points()` 已经从 `quant.market_token_block_close` 读取 block 级数据。
- `PricePoint` 已经包含 `volume` 和 `trade_count`。
- `_orderfilled_fill_decision()` 已经使用：
  - `block_volume`
  - `liquidity_cap_pct`
  - `min_fill_pct`
  - `allow_partial_fill`
  - `max_position_notional`
- `quant_backtest_trades` 已经记录：
  - `requested_notional`
  - `filled_notional`
  - `requested_size`
  - `filled_size`
  - `unfilled_size`
  - `fill_pct`
  - `fill_status`
  - `fill_probability`
  - `block_volume`
  - `trade_count`
  - `available_notional`
  - `execution_source`
  - `fee_cost`
  - `slippage_cost`
  - `execution_cost`
- `quant_backtest_events` 已经记录：
  - `open`
  - `close`
  - `fill_rejected`
  - `exit_rejected`
  - `force_close_rejected`
- 前端 Strategy Tester 已经有：
  - fee
  - slippage
  - liquidity cap
  - min fill
  - partial fill
  - execution mode
  - CLOB depth / OrderFilled / Legacy 模式切换

因此 Phase 1 的重点是补缺口，而不是重写主路径。

## Phase 1A: 固化数据快路径

### 目标

保证在线回测不会从 14 亿级 raw `orderfilled_fact` 现场 group all markets，而是稳定走 key/index/聚合表。

### 当前状态

当前主路径已经基本正确：

```text
quant.market_token_block_close -> PricePoint -> simulate_strategy
```

Postgres 聚合表已有适合回测的键：

```text
PRIMARY KEY (token_id, block_number)
INDEX (market_slug, token_side, block_number)
INDEX (market_id, token_side, block_number)
```

ClickHouse raw fact 表适合受限查询：

```text
ORDER BY (market_id, token_id, block_number, log_index, tx_hash)
```

### 要做

1. 在 run meta 中记录真实数据访问路径：
   - `source_table`
   - `access_path`
   - `token_id`
   - `market_slug`
   - `token_side`
   - `from_block`
   - `to_block`
   - `actual_first_block`
   - `actual_last_block`
   - `row_count`
   - `data_version`
   - `query_guard_version`

2. 如果 payload 未传 `token_id`，后端要明确解析到唯一 token/outcome。

3. 如果 market_slug + token_side 对应多个 token，要返回明确错误或要求前端传 token_id，不能静默混读。

4. 后端增加 query guard：
   - 在线回测默认只能读 `quant.market_token_block_close`。
   - 需要 raw OrderFilled 明细时，必须带 `market_id + token_id + block_number range`。
   - 禁止在线路径触发跨市场 raw fact 聚合。

5. 测试覆盖：
   - 单 market 回测使用 `market_token_block_close`。
   - run meta 包含访问路径和 data version。
   - 不带 block range 时仍然有实际 observed block range。

### 验收

- 任意单 market 回测都能说明用了哪张表、哪个 key、哪个 block range。
- 普通回测不访问全量 `orderfilled_fact`。
- run artifact 能复现同一次回测输入。

## Phase 1B: 订单生命周期模型

### 目标

把 signal 和 trade 分开，让未成交订单成为一等数据，而不是只藏在 `quant_backtest_events.meta` 里。

### 当前状态

现在已有 `quant_backtest_events`，但它更像事件日志，不是正式订单表。

当前问题：

- `fill_rejected` 是 event，不是 order。
- `open` 同时承担 signal/order/fill 的语义。
- 未成交订单没有结构化字段用于统计。
- Strategy Tester 主要围绕 closed trades 展示。

### 新增表

新增 `quant.quant_backtest_orders`。

建议字段：

```text
run_id
order_id
signal_index
trade_id
x_axis
signal_x
submit_x
decision_price
requested_price
side
role
order_type
status
requested_size
requested_notional
filled_size
filled_notional
unfilled_size
avg_fill_price
fill_probability
fill_pct
block_volume
trade_count
available_notional
fee_cost
slippage_cost
execution_cost
latency_blocks
latency_seconds
no_fill_reason
execution_source
meta
created_at
```

### 状态枚举

第一阶段至少支持：

- `SIGNAL`
- `SUBMITTED`
- `FILLED`
- `PARTIAL_FILLED`
- `NO_FILL`
- `REJECTED`
- `CANCELED`
- `CANCEL_FAILED`

### 要做

1. 在 `simulate_strategy()` 中，每次 entry/exit signal 都生成 order intent。

2. `_fill_decision()` 返回结果后，生成对应 order row。

3. 继续保留 `quant_backtest_events`，但它不再是唯一事实来源。

4. `replace_backtest_results()` 同步写入 `quant_backtest_orders`。

5. API 增加：
   - `get_backtest_orders(run_id)`
   - orders summary

6. 前端 Strategy Tester 增加 Fill Quality 区域：
   - signal count
   - submitted count
   - filled count
   - partial fill count
   - no fill count
   - rejected count
   - avg fill probability

### 验收

- 未成交订单不会丢失。
- 回测报告能统计 No Fill / Partial Fill。
- Strategy Tester 不再只展示成交后的 trades。
- 每个 trade 可以追溯到 order。

## Phase 1C: OrderFilled 成交模型升级

### 目标

把默认执行口径从“价格信号触发后用 volume/probability 估计成交”改成“先挂限价单，再用后续真实 OrderFilled 成交价穿价回放成交”。

这条是 Polymarket 回测的核心约束：多数实盘成交来自 limit order。不能把 `price >= entry_threshold` 这种价格走势信号直接当成可交易机会。

默认模型：

```text
BUY limit L:
  订单先挂出。
  只有后续真实 trade_price <= L，才认为买单被吃进。

SELL limit L:
  订单先挂出。
  只有后续真实 trade_price >= L，才认为卖单被吃出。
```

小资金 Phase 1 假设：

- 只要真实成交价穿过 limit price，就允许成交。
- 不做全量 L2 queue position。
- 不因为资金太大造成不可成交；大资金、partial fill、queue miss 后续再增强。
- `block_volume/trade_count` 只作为审计信息和可选压力模型，不再是默认成交必要条件。

卖出和结算：

- 中途能卖出时，用真实穿价成交价或更保守的 limit price 记录 `trade_exit_pnl`。
- 中途卖不出时，继续持仓。
- 回测结束不能默认用最后价格 `FORCE_CLOSE`。
- 已结算市场必须按 YES payoff `0/1` 结算，生成 `settlement_pnl`。
- 未提供 settlement value 且价格没有明确 0/1 时，仓位应保持 open/unresolved，不能伪造成已成交退出。

### 当前状态

当前核心逻辑：

```text
available_notional = block_volume * liquidity_cap_pct / 100
fill_probability = min(100, available_notional / target_notional)
```

这已经比 close price 必成交更真实，但还缺：

- 默认仍偏 signal backtest，不是 limit order replay
- `FORCE_CLOSE` 使用最后价格退出，容易把不可卖出的尾盘价格当成可成交价格
- 缺少 `trade_exit_pnl` vs `settlement_pnl` 的分解
- profile 分档
- order role
- latency blocks
- adverse slippage cents
- fill probability haircut
- no fill reason 结构化
- markout

### 新增参数

新增到 `quant_backtest_parameters`：

```text
execution_price_mode TEXT DEFAULT 'ORDERFILLED_LIMIT_REPLAY'
buy_limit_price NUMERIC(20, 10)
sell_limit_price NUMERIC(20, 10)
settlement_value NUMERIC(20, 10)
execution_profile TEXT DEFAULT 'realistic'
order_role TEXT DEFAULT 'taker'
latency_blocks BIGINT DEFAULT 0
adverse_slippage_cents NUMERIC(20, 10) DEFAULT 0
fill_probability_haircut_pct NUMERIC(20, 10) DEFAULT 0
final_valuation_mode TEXT DEFAULT 'SETTLEMENT'
```

说明：

- `participation_cap_pct` 第一阶段不单独新建，先复用现有 `liquidity_cap_pct`，避免参数重复。
- `execution_profile` 用于一键设置参数组合，但仍允许用户手动覆盖。
- `entry_threshold` 在新模式下只兼容旧参数，默认映射为 `buy_limit_price`。
- `exit_threshold/take_profit` 不再自动代表可成交卖价；新模式优先使用 `sell_limit_price`。
- 如果 `settlement_value` 未显式传入，但最后价格已经是 `0` 或 `1`，可以把最后价格作为 settlement payoff。
- 如果 `settlement_value` 不存在且最后价格不是 `0/1`，不得用最后价格强平。

### Profile 建议

`optimistic`：

- `fill_probability_haircut_pct = 0`
- `adverse_slippage_cents = 0`
- `latency_blocks = 0`

`realistic`：

- `fill_probability_haircut_pct = 20`
- `adverse_slippage_cents = 0.005` 到 `0.01`
- `latency_blocks = 0` 或 `1`

`conservative`：

- `fill_probability_haircut_pct = 50`
- `adverse_slippage_cents = 0.01` 到 `0.02`
- `latency_blocks = 1`

`stress`：

- `fill_probability_haircut_pct >= 50`
- `adverse_slippage_cents >= 0.02`
- `latency_blocks >= 1`
- 可配合 `liquidity_cap_pct <= 50`

### 成交模型输入

- limit order side
- buy_limit_price
- sell_limit_price
- 后续 block 的真实 OrderFilled trade/block close price
- latency blocks
- block volume
- trade count
- settlement_value
- time-to-expiry
- execution profile
- order role

第一阶段必须先实现 limit price 穿价和 settlement payoff；`time-to-expiry` 后续再接 metadata。

### 输出字段

写入 orders/trades：

- limit price
- crossing price
- crossing block
- trade_exit_pnl
- settlement_pnl
- actual fill size
- actual fill price
- slippage cost
- no fill reason
- markout after N blocks

### 要做

1. 扩展 `BacktestParameters`。

2. 扩展 `parse_parameters()`、parameter snapshot、fingerprint。

3. 新增 `ORDERFILLED_LIMIT_REPLAY` 执行模式：
   - BUY 只有 `trade_price <= buy_limit_price` 才能成交。
   - SELL 只有 `trade_price >= sell_limit_price` 才能成交。
   - 默认小资金假设下，穿价即成交。
   - 成交价默认用 limit price；如后续需要更精细，可扩展为 midpoint/worse-of-limit-and-trade。
   - 输出结构化 `no_fill_reason`。

4. metrics 增加：
   - execution_profile
   - execution_mode
   - no_fill_count
   - rejected_count
   - avg_slippage_cost
   - avg_latency_blocks
   - trade_exit_pnl
   - settlement_pnl

5. 前端 Settings 增加 profile/role/latency/adverse slippage。

### 验收

- `ORDERFILLED_LIMIT_REPLAY` 成为默认口径。
- 同一 market 可以对比 limit replay 与旧 `ORDERFILLED` volume/probability 模式。
- BUY 不因为当前价格达到信号阈值就成交，必须等后续价格 `<= buy_limit_price`。
- SELL 不因为触发退出信号就成交，必须等后续价格 `>= sell_limit_price`。
- 如果 SELL 没穿价，不能用最后 price force close。
- 若有 settlement value，最终按 `0/1` 结算。
- 报告必须区分 `trade_exit_pnl` 和 `settlement_pnl`。
- No Fill 和 slippage 的原因可解释。

## Phase 1D: Ledger / Cashflow MVP

### 目标

让净收益开始从账本推导，而不是只依赖 trade entry/exit price 差。

### 当前状态

当前 `quant_backtest_trades` 已记录 fee/slippage/execution_cost，但 PnL 仍主要由 `_close_trade()` 算出来。

### 新增表

新增 `quant.quant_backtest_ledger`。

建议字段：

```text
run_id
ledger_id
order_id
trade_id
event_type
x_axis
x_value
market_slug
token_side
shares_delta
cash_delta
fee
rebate
slippage_cost
execution_cost
realized_pnl
position_after
cash_after
price
source
meta
created_at
```

### 事件类型

第一阶段支持：

- `BUY`
- `SELL`
- `FEE`
- `REBATE`
- `MARK_TO_MARKET`
- `SETTLEMENT`

兼容旧模式：

- `FORCE_CLOSE`

后续扩展：

- `SPLIT`
- `MERGE`
- `REDEEM`
- `REFUND`

### 要做

1. open order fill 时生成 `BUY` ledger row。

2. close order fill 时生成 `SELL` ledger row。

3. fee/slippage 可以先合并在 BUY/SELL row，也可以拆成 `FEE` / `SLIPPAGE` row。第一阶段建议拆出独立字段，是否拆 row 后续再定。

4. 同一笔 entry 如果被多次 partial exit，ledger 只能记录一次 `BUY`，然后记录多条 `SELL`。不能因为 trade table 拆成多条 closed leg，就重复扣买入现金。

5. `final_valuation_mode=SETTLEMENT` 是 Polymarket 默认口径。回测结束时残余仓位不能用最后 price 强制平仓；必须：
   - 如果已知 settlement payoff，按 `0/1` 结算。
   - 如果未知 settlement payoff，保留 open/unresolved 仓位，只给 mark-to-market，不给 realized exit PnL。
   - `FORCE_CLOSE` 只能作为 legacy/diagnostic 模式显式启用。

6. 生成 running state：
   - `position_after`
   - `cash_after`

7. metrics 的 `net_profit` 增加 ledger 校验口径：
   - `net_profit_trade`
   - `net_profit_ledger`
   - `trade_exit_pnl`
   - `settlement_pnl`
   - `ledger_diff`

8. API 增加：
   - `get_backtest_ledger(run_id)`
   - ledger summary

9. 前端 Properties 或 Fill Quality 增加 ledger 区域。

### 验收

- 每个 closed trade 都能追溯到 ledger rows。
- ledger 汇总和 trade PnL 差异在可解释范围内。
- fee/slippage/execution cost 单独可见。
- 后续接真实链上 `SPLIT/MERGE/REDEEM/REBATE` 不需要推翻表结构。

## Phase 1E: 测试和 Smoke

### 单元测试

新增或扩展：

- `tests/test_quant_backtest_frameworks.py`
- `tests/test_quant_execution.py`

覆盖：

1. `ORDERFILLED` 有 volume 时 partial fill 正确。
2. no volume 时生成 order lifecycle `NO_FILL/REJECTED`。
3. `execution_profile` 改变 fill probability/slippage。
4. ledger 汇总和 trade PnL 可对齐。
5. run meta 包含 source table/access path/data version。
6. 起始资金 100 USDC 时，系统能正确判断是否买得起。
7. 买入时能记录成本摩擦：
   - fee
   - slippage
   - execution cost
   - filled/not filled/partial filled
8. 买入时能记录成交概率：
   - fill probability
   - block volume
   - trade count
   - available notional
   - no fill reason
9. 买入后能根据后续 `orderfilled_block_close` 价格变化更新持仓浮盈亏。
10. 总账户 PnL 能同时反映：
    - cash balance
    - open position value
    - realized PnL
    - unrealized PnL
    - total equity
11. `ORDERFILLED_LIMIT_REPLAY` 买入必须等待后续真实价格 `<= buy_limit_price`。
12. `ORDERFILLED_LIMIT_REPLAY` 卖出必须等待后续真实价格 `>= sell_limit_price`。
13. 卖出未穿价时，不能用最后价格退出；已知结算结果时按 `0/1` 生成 `SETTLEMENT` ledger。
14. 回测结果必须拆分：
    - `trade_exit_pnl`
    - `settlement_pnl`

### Smoke run

选择一个已有真实数据的 market，例如：

- `nba-nyk-sas-2026-06-05-spread-home-6pt5`
- 或当前默认 FIFA event 的单 outcome。

验收：

- run succeeded。
- rows_processed > 0。
- orders > 0。
- ledger rows > 0。
- metrics 有 fill/no-fill/ledger/data_quality。
- 不访问全量 raw `orderfilled_fact`。
- 使用 `initial_capital=100`。
- 至少生成一笔 BUY order intent。
- 如果资金不足、min fill 不满足或 block volume 不足，订单必须进入 `NO_FILL/REJECTED/PARTIAL_FILLED`，不能静默消失。
- 如果成功买入，ledger 必须记录：
  - BUY cash delta
  - shares delta
  - fee/slippage/execution cost
  - cash_after
  - position_after
- 买入后每个后续价格点的 equity/mark-to-market 能体现该仓位的浮盈亏。
- 最终报告能区分：
  - realized PnL
  - unrealized PnL
  - total equity
  - account-level PnL

## 交付物

第一阶段完成后，应包含：

1. `quant_backtest_orders` 表。
2. `quant_backtest_ledger` 表。
3. 扩展后的 `quant_backtest_parameters`。
4. 更新后的 backtest engine。
5. 更新后的 API read/write。
6. Strategy Tester 中的 Fill Quality / Ledger / Execution Profile 展示。
7. 单元测试。
8. 一个 `initial_capital=100` 的真实 market smoke run 结果。

## 成功标准

Phase 1 完成后，系统应该能回答这些问题：

- 这个策略触发了多少次 signal？
- 每个 signal 是否提交了订单？
- 每个订单为什么成交、部分成交或没有成交？
- 每笔成交的价格、数量、滑点、费用从哪里来？
- 净收益是否能从 ledger 推导？
- 起始资金 100 USDC 时，系统是否正确判断买入能力？
- 买入后，持仓浮盈亏和总账户 PnL 是否随 `orderfilled_block_close` 价格变化更新？
- 数据来自哪张表、哪个 block range、哪个 data version？
- realistic 和 stress 下结果差多少？

如果这些问题都能回答，Phase 1 才算完成。
