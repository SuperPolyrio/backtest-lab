我理解你这里问的是：**在 LOB 不齐全、只能做 `OrderFilled-only` 回测时，`OrderFilled` 应该怎么处理，价格数据怎么来，以及要不要做 blocknumber 级别 OHLCV。**

答案是：

**要做 blocknumber 级别 OHLCV，但它只能是“摘要层 / 信号层 / 估值层”，不能是“成交执行层”。真正驱动成交的应该是标准化后的 `OrderFilled trade prints`。**

---

# 1. 你的 `OrderFilled` 数据应该分成三层

不要直接拿原始 `OrderFilled` 去回测。你应该把它处理成三张核心数据表。

## 第一层：`raw_orderfilled`

这一层保留原始链上事件，不做太多解释。

字段类似：

```text
block_number
block_time
tx_hash
tx_index
log_index
orderHash
maker
taker
makerAssetId
takerAssetId
makerAmountFilled
takerAmountFilled
fee
contract_address
```

这张表的作用是：

```text
审计
回溯
重新 normalize
排查异常
```

不要在策略里直接用它。

---

## 第二层：`maker_fill_ticks`

这一层是把每条 `OrderFilled` 规范化成 maker 侧成交记录。

字段：

```text
fill_id
block_number
block_time
tx_hash
tx_index
log_index
order_hash
market_id / condition_id
asset_id
outcome: YES / NO
price
size
notional
passive_side: BUY / SELL
aggressor_side: BUY / SELL
maker
taker
fee
```

这里的方向很重要。

一般规则是：

```text
makerAssetId == 0:
    maker 用 USDC 买 outcome token
    passive_side = BUY
    aggressor_side = SELL
    asset_id = takerAssetId
    size = takerAmountFilled
    price = makerAmountFilled / takerAmountFilled

takerAssetId == 0:
    maker 卖 outcome token 换 USDC
    passive_side = SELL
    aggressor_side = BUY
    asset_id = makerAssetId
    size = makerAmountFilled
    price = takerAmountFilled / makerAmountFilled
```

Polymarket / 相关数据分析资料里也说明，`OrderFilled` 事件包含 `makerAssetID`、`takerAssetID`、`makerAmountFilled`、`takerAmountFilled` 等字段；如果 `makerAssetId` 是 0，maker 是用 USDC 买 outcome token，反之则是卖 outcome token。([Paradigm][1])

这张表用于：

```text
maker 侧成交分析
钱包成交复盘
订单哈希级审计
和链上原始事件对账
```

但注意：**这张表还不一定适合直接当市场成交量。**

因为一次撮合里可能有多个 `OrderFilled`。研究论文也说明，Polymarket 每次 matched transaction 会有 `OrderFilled` 和 `OrdersMatched` 两类事件，`OrderFilled` 是每个 matched order 的记录，而 `OrdersMatched` 是交易层面的摘要。([arXiv][2])

---

## 第三层：`trade_prints_one_sided`

这一层才是你做 `OrderFilled-only` 执行模型最应该用的“成交 tape”。

它的目标是把多个 maker-level `OrderFilled` 聚合成一条或多条**不会双算的市场成交记录**。

字段：

```text
trade_id
block_number
block_time
tx_hash
trade_group_id
market_id
asset_id
outcome
price
size
notional
aggressor_side
passive_side
source_orderfilled_ids
```

为什么要这层？

因为你不能简单地把所有 `OrderFilled` 行加起来当 volume。Paradigm 的 Polymarket volume 分析就指出，Polymarket 里直接加总 `OrderFilled` 很容易双算成交量，因为一笔 matched transaction 可能同时出现买方、卖方或多 maker 的事件记录；更合理的是用 one-sided volume。([Paradigm][1])

所以你的数据处理应该是：

```text
raw_orderfilled
    ↓ normalize
maker_fill_ticks
    ↓ group / dedupe / one-sided volume
trade_prints_one_sided
```

`OrderFilled-only` 回测的执行层，应该主要使用：

```text
trade_prints_one_sided
```

而不是直接用：

```text
raw_orderfilled
maker_fill_ticks
block OHLCV
```

---

# 2. 那价格数据用什么？

分用途。

## 2.1 执行价格：用 `trade_prints_one_sided` 的成交价格

在没有 LOB 的情况下，你最可信的价格就是：

```text
真实发生过的 OrderFilled-derived trade price
```

也就是：

```text
price = USDC amount / outcome token amount
```

但是执行时不能直接说：

```text
我的订单全部按这个 price 成交
```

而应该说：

```text
我的订单只能参与 arrival_ts 之后、方向相同、价格满足 limit 的真实历史成交流的一小部分。
```

例如：

```text
我的策略在 block 100 发出 BUY YES limit 0.53
延迟后到达 block 101

后面 trade tape 有：
block 102 BUY @ 0.52 size 2000
block 103 BUY @ 0.53 size 1000
block 104 BUY @ 0.55 size 3000
```

那你的 BUY 订单可以考虑参与：

```text
block 102 BUY @ 0.52
block 103 BUY @ 0.53
```

不能参与：

```text
block 104 BUY @ 0.55
```

因为超过了 limit。

如果设置 `participation_rate = 2.5%`：

```text
block 102 可分配容量 = 2000 * 2.5% = 50
block 103 可分配容量 = 1000 * 2.5% = 25
```

所以最多成交：

```text
75 shares
```

这就是 `OrderFilled-only` 的核心：**不是“价格到了就成交”，而是“只能分配真实历史成交量的一小部分”。**

---

## 2.2 信号价格：可以用 `OrderFilled` 派生的 trade price / OHLCV

策略信号可以用：

```text
last trade price
block OHLCV
1m / 5m OHLCV
成交量
成交方向 imbalance
VWAP
价格动量
成交密度
```

这些都可以从 `trade_prints_one_sided` 派生出来。

但是它们只能用于：

```text
signal
feature
research
valuation
plot
```

不能直接用于：

```text
execution fill
```

你之前文档里的核心原则也是这个：`OrderFilled` 应该被做成 `FillTick / TradeTick / execution evidence`，而 OHLCV / close price 不应该驱动真实成交。

---

## 2.3 估值价格：可以用 last trade，但必须带 stale 标记

如果没有 LOB，你没有 mid price，也没有 best bid/ask。那组合估值只能用：

```text
last_trade_price
last_valid_trade_price
settlement value
derived yes/no price
```

但必须加：

```text
price_age_blocks
price_age_seconds
is_stale_price
```

比如：

```text
last trade 是 6 小时前的 0.62
当前没有新成交
```

你可以用 0.62 做临时 mark，但报告里必须标记：

```text
mark_source = last_trade
mark_stale = true
price_age = 6h
```

不要把 stale last price 当成实时可成交价格。

---

# 3. 每个 blocknumber 有很多 `OrderFilled` 怎么办？

不要先粗暴聚合成一个 close。

正确处理顺序是：

```text
block_number
  └── tx_index
        └── log_index
```

也就是说，同一个 block 里多笔 `OrderFilled` 要按：

```text
block_number ASC
tx_index ASC
log_index ASC
```

排序。

如果你没有 `tx_index`，至少用：

```text
block_number ASC
tx_hash 的稳定排序
log_index ASC
```

但最好补 `transaction_index`，否则同 block 内事件顺序不够严格。

---

## 3.1 同一个 block 内的多笔成交，执行层怎么处理？

执行层不要用 block OHLCV，而要用 trade prints 逐笔处理。

例如某个 block 里有：

```text
block 1000:

tx A log 10: BUY  @ 0.51 size 500
tx A log 11: BUY  @ 0.52 size 300
tx B log 05: SELL @ 0.50 size 1000
tx C log 02: BUY  @ 0.53 size 200
```

你的 trade tape 应该保留这些顺序：

```text
1000 / A / 10
1000 / A / 11
1000 / B / 05
1000 / C / 02
```

执行模型逐条消费：

```text
订单 arrival_block <= 1000
方向满足
价格满足
capacity 未被别的模拟订单消耗
```

这样你不会丢掉同一个 block 内的成交顺序和方向。

---

## 3.2 OHLCV 层怎么处理同 block 多笔成交？

如果你要生成 block-level OHLCV，那就按排序后的 trade prints 聚合：

```text
open  = 该 block 第一笔 trade price
high  = 该 block 最高 trade price
low   = 该 block 最低 trade price
close = 该 block 最后一笔 trade price
volume = one-sided size 之和
notional = one-sided notional 之和
vwap = notional / volume
trade_count = trade prints 数量
```

注意：

```text
volume 必须用 one-sided volume
不能直接 sum raw OrderFilled
```

---

# 4. 有的 block 里这个 market 没交易数据怎么办？

这很正常。

没有交易的 block，代表：

```text
没有新的成交证据
```

它不代表：

```text
价格等于上一 block
可以成交
盘口还在
```

所以分两种表处理。

---

## 4.1 稀疏 trade bars：只在有交易的 block 生成 bar

这是最干净的。

例如：

```text
block 1000 有交易 -> 有一行
block 1001 没交易 -> 没有行
block 1002 没交易 -> 没有行
block 1003 有交易 -> 有一行
```

这张表叫：

```text
block_trade_bars_sparse
```

字段：

```text
market_id
asset_id
block_number
block_time
open
high
low
close
vwap
volume
notional
trade_count
```

这是最适合做：

```text
真实成交统计
成交密度
成交价格路径
成交量曲线
```

---

## 4.2 稠密 block bars：补齐每个 block，但必须标记 no_trade

如果你的策略引擎要求每个 block 都有一行，可以生成 dense bars。

例如：

```text
block 1000 有交易：
    open=0.50 high=0.52 low=0.50 close=0.52 volume=1000 no_trade=false

block 1001 无交易：
    open=0.52 high=0.52 low=0.52 close=0.52 volume=0 no_trade=true stale=true

block 1002 无交易：
    open=0.52 high=0.52 low=0.52 close=0.52 volume=0 no_trade=true stale=true
```

但你必须理解：

```text
block 1001 的 OHLC 不是成交出来的价格
只是 carry-forward mark
```

所以字段要写清楚：

```text
is_trade_bar = false
is_carry_forward = true
volume = 0
trade_count = 0
price_age_blocks = 当前 block - last_trade_block
price_age_seconds = 当前时间 - last_trade_time
```

我建议不要把这种表叫普通 OHLCV，最好叫：

```text
block_marks_dense
```

或者：

```text
block_ohlcv_with_stale_marks
```

避免以后误用。

---

# 5. 要不要做 blocknumber 级别 OHLCV？

**要做，但不要拿它驱动成交。**

我建议做三类价格数据表。

---

## 表 A：`trade_prints_one_sided`

这是执行层主数据。

粒度：

```text
一笔真实市场成交 / 一组不会双算的成交
```

用途：

```text
OrderFilled-only 执行
成交证据
participation capacity
成交方向
成交审计
```

这是最重要的表。

---

## 表 B：`block_trade_bars_sparse`

这是 block 级真实成交摘要。

粒度：

```text
market_id + asset_id + block_number
只包含有交易的 block
```

用途：

```text
快速查询
画图
block 级信号
成交量统计
生成更高周期 bars
```

字段：

```text
open
high
low
close
vwap
volume
notional
trade_count
buy_volume
sell_volume
buy_count
sell_count
first_trade_id
last_trade_id
```

这个很值得做。

---

## 表 C：`time_bars_1m_5m`

这是时间级研究数据。

粒度：

```text
1m / 5m / 15m / 1h
```

用途：

```text
策略信号
统计研究
参数扫描
图表
```

生成方式：

```text
从 trade_prints_one_sided 或 block_trade_bars_sparse 聚合
```

注意：

```text
无成交时间段可以 volume=0，并 forward-fill close 作为 mark
但必须标记 stale
```

---

# 6. 不能只做 block OHLCV 的原因

如果你只做：

```text
block_number -> OHLCV
```

然后用它回测，会遇到几个问题。

## 问题一：同一 block 内多笔成交会丢方向

比如：

```text
block 1000:
BUY  @ 0.60 size 10
SELL @ 0.50 size 10000
```

OHLCV 可能是：

```text
O=0.60
H=0.60
L=0.50
C=0.50
V=10010
```

但你不知道：

```text
BUY 成交流很小
SELL 成交流很大
你的 BUY taker 到底能不能成交
你的 SELL taker 到底能不能成交
```

执行层需要方向和逐笔容量，所以不能只用 OHLCV。

---

## 问题二：OHLCV 会让你误以为 high/low 可成交

如果某个 block：

```text
high = 0.70
volume = 10000
```

但实际：

```text
0.70 只成交了 5 shares
其余 9995 shares 都在 0.55
```

你用 OHLCV 会误以为可以在 0.70 附近大量成交。

逐笔 `trade_prints` 不会犯这个错。

---

## 问题三：无成交 block 的 forward-fill 不是真实价格

如果某个 market 3 小时没有成交，你 forward-fill 出来：

```text
close = 0.62
```

这不是可成交价格，只是 last mark。

所以 execution 不能用它。

---

# 7. OrderFilled-only 回测里的执行逻辑应该怎样用这些价格？

## 7.1 Taker

Taker 不用 block OHLCV。

Taker 用：

```text
trade_prints_one_sided
```

逻辑：

```text
BUY order:
    arrival_ts / arrival_block 之后
    找 aggressor_side = BUY 的 trade prints
    price <= limit_price
    在 execution_horizon 内
    每笔最多吃 participation_rate * trade_size

SELL order:
    arrival 后
    找 aggressor_side = SELL 的 trade prints
    price >= limit_price
    每笔最多吃 participation_rate * trade_size
```

没有符合条件的 trade print：

```text
不成交
```

不能用：

```text
block close
last price
previous block price
```

来成交。

---

## 7.2 Maker

OrderFilled-only 下 maker 要更保守。

没有 LOB 时，你不知道 queue，所以主模型建议：

```text
maker strict:
    不成交，只记录 candidate

maker conservative:
    设置 phantom_queue
    后续相反方向 trade prints 先消耗 phantom_queue
    queue 清零后才允许成交
```

BUY maker：

```text
BUY @ P
只能被 aggressor_side = SELL 且 price <= P 的 trade prints 推进
```

SELL maker：

```text
SELL @ P
只能被 aggressor_side = BUY 且 price >= P 的 trade prints 推进
```

这里也不需要 block OHLCV。

---

# 8. 如果某个 block 没交易，策略怎么运行？

要看策略类型。

## 8.1 信号层

可以用 dense marks：

```text
上一笔成交价 forward-fill
price_age_blocks
price_age_seconds
stale flag
```

比如：

```text
如果 last trade price > 0.60 且 price_age < 30min
```

可以作为信号。

但如果：

```text
price_age 太大
```

这个信号应该无效或降权。

---

## 8.2 执行层

无成交 block 不提供成交容量。

所以：

```text
没有 trade print = 不能 fill
```

如果订单在 block 1000 到达，block 1001、1002 没有交易，block 1003 有合格 trade，那么它可以在 block 1003 考虑成交。

如果一直没有合格 trade 到 horizon 结束：

```text
订单 unfilled / expired / cancelled
```

---

# 9. blocknumber OHLCV 应该怎么定义？

建议你做一个明确的 schema。

```text
block_trade_bars_sparse
```

字段：

```text
chain_id
market_id
condition_id
asset_id
outcome
block_number
block_time_start
block_time_end

open_price
high_price
low_price
close_price
vwap_price

volume_shares
notional_usdc
trade_count

buy_volume_shares
sell_volume_shares
buy_notional_usdc
sell_notional_usdc
buy_count
sell_count

first_trade_id
last_trade_id

source = orderfilled_one_sided
is_sparse = true
```

对于同一 block 内的顺序：

```text
open = first trade by tx_index/log_index
close = last trade by tx_index/log_index
```

如果同一 block 有多 asset，比如 YES 和 NO：

```text
YES token 一张 bar
NO token 一张 bar
```

不要把 YES 和 NO 直接混在同一个 asset bar 里。

如果你还要 canonical YES probability，可以另做：

```text
canonical_yes_block_bars
```

规则：

```text
YES trade price -> yes_price = price
NO trade price  -> yes_price = 1 - price
```

但这张表是研究用的，不是执行用的。

---

# 10. 每个 block 多个 `OrderFilled` 的聚合细节

你需要先决定“成交单位”。

## 10.1 maker-level aggregation

如果你只是按 `OrderFilled` 行聚合：

```text
volume = sum(size)
```

可能会双算或至少会把一笔 taker sweep 拆得很碎。

这对 maker fill 分析有用，但对市场 volume 要小心。

---

## 10.2 tx-level one-sided aggregation

更适合 OHLCV 和容量。

按：

```text
tx_hash
asset_id
aggressor_side
price
```

聚合，或者如果你能解析 `OrdersMatched`，按 `OrdersMatched` 做交易组。

结果：

```text
一笔 tx 里同一个 asset、同方向、同价格的多个 maker fills
合并成一条 trade print
```

如果同一个 tx sweep 多个价格：

```text
保留多条 price-level trade prints
```

比如：

```text
tx 0xabc:
    BUY 100 @ 0.51
    BUY 200 @ 0.52
    BUY 300 @ 0.53
```

不要合并成一条 `BUY 600 @ 0.52`，除非是做 VWAP 统计。执行层最好保留价格档。

---

# 11. 有的 market 某些 block 没交易，要不要补 0 volume bar？

我的建议：

```text
执行用：不要补。
信号/图表用：可以补，但必须 stale 标记。
```

所以两张表：

## 稀疏表

```text
block_trade_bars_sparse
```

只含有交易的 block。

## 稠密表

```text
block_marks_dense
```

每个 block 都有，没交易时：

```text
volume = 0
trade_count = 0
open/high/low/close = previous close
is_carry_forward = true
is_trade_observed = false
price_age_blocks > 0
```

不要把稠密表喂给执行引擎。

---

# 12. 推荐的数据流

你的 pipeline 可以这样写：

```text
raw_orderfilled
    ↓
normalize side / price / size / asset_id
    ↓
maker_fill_ticks
    ↓
dedupe + group + one-sided volume
    ↓
trade_prints_one_sided
    ↓
    ├── block_trade_bars_sparse
    ├── time_bars_1m_5m
    ├── block_marks_dense
    └── orderfilled_only_execution_engine
```

执行引擎只读：

```text
trade_prints_one_sided
```

信号可以读：

```text
block_trade_bars_sparse
time_bars
block_marks_dense with stale control
```

估值可以读：

```text
last_trade_mark
settlement mark
stale-aware marks
```

---

# 13. 一个具体例子

假设某个 market 的 YES token 有这些 `trade_prints_one_sided`：

```text
block 100, tx 1, log 10: BUY  @ 0.51 size 100
block 100, tx 1, log 11: BUY  @ 0.52 size 200
block 100, tx 2, log 05: SELL @ 0.50 size 500
block 103, tx 1, log 01: BUY  @ 0.53 size 1000
```

生成 block OHLCV：

```text
block 100:
open  = 0.51
high  = 0.52
low   = 0.50
close = 0.50
volume = 800
buy_volume = 300
sell_volume = 500
trade_count = 3

block 101:
无 sparse bar

block 102:
无 sparse bar

block 103:
open = high = low = close = 0.53
volume = 1000
buy_volume = 1000
sell_volume = 0
trade_count = 1
```

如果做 dense marks：

```text
block 101:
close = 0.50
volume = 0
is_carry_forward = true
price_age_blocks = 1

block 102:
close = 0.50
volume = 0
is_carry_forward = true
price_age_blocks = 2
```

现在策略在 block 101 产生：

```text
BUY YES limit 0.52
size = 1000
participation_rate = 2.5%
execution_horizon = 5 blocks
```

执行层不会用 block 101 的 carry-forward 0.50 成交。

它会向未来找：

```text
block 103 BUY @ 0.53 size 1000
```

但是：

```text
0.53 > limit 0.52
```

所以不成交。

如果策略 limit 是 0.54，则：

```text
capacity = 1000 * 2.5% = 25
fill = 25 @ 0.53 或 0.54-with-buffer
remaining = 975
```

这就是正确的 OrderFilled-only 行为。

---

# 14. 最终建议

你的问题可以定成这几个结论：

## 结论 1：`OrderFilled` 要先标准化，不要直接回测

你需要：

```text
raw_orderfilled
maker_fill_ticks
trade_prints_one_sided
```

执行层主要使用：

```text
trade_prints_one_sided
```

---

## 结论 2：价格数据主要来自 `OrderFilled` 成交价

但是它分三种用途：

```text
执行：
    用逐笔 trade prints，不用 OHLCV。

信号：
    可以用 block/time OHLCV、VWAP、last price。

估值：
    可以用 last trade mark，但要 stale flag。
```

---

## 结论 3：同一个 block 有很多 `OrderFilled` 时，不能先压成一条

必须按：

```text
block_number + tx_index + log_index
```

处理。

OHLCV 可以后生成，但执行必须用逐笔成交 tape。

---

## 结论 4：某个 block 没交易时，不能凭空成交

无交易 block 可以 forward-fill 做 mark，但：

```text
volume = 0
trade_count = 0
is_carry_forward = true
stale = true
```

执行层不能用这类价格成交。

---

## 结论 5：要做 blocknumber 级别 OHLCV，但它不是 execution truth

你应该做：

```text
block_trade_bars_sparse
block_marks_dense
time_bars
```

但真正成交模型应该是：

```text
OrderFilled trade-tape participation model
```

一句话：

**OrderFilled-only 回测里，OHLCV 是地图，trade prints 才是路；没有 trade print 的 block，不能假装有路可走。**

[1]: https://www.paradigm.xyz/writing/polymarket-volume-is-being-double-counted?utm_source=chatgpt.com "Polymarket Volume Is Being Double-Counted - Paradigm.xyz"
[2]: https://arxiv.org/html/2603.03136v2?utm_source=chatgpt.com "Polymarket in the 2024 U.S. Presidential Election"
