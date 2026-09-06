结论先说清楚：

**PMXT v2 + 你自己的 `OrderFilled` 数据，足够做一个严肃的 `L2 + OrderFilled` 执行模型；不够做 L3 FIFO；也不够让所有 Polymarket market 都进入同一精度的回测。**

也就是说，你应该把市场分层处理：

```text
有 PMXT LOB + OrderFilled：
    可以进入主回测结果，使用 L2 queue-aware execution。

只有 OrderFilled，没有 LOB：
    只能做成交证据 / trade-tape 回放，不能做严格 taker/maker 执行。

只有 OHLCV / price history：
    只能做 signal research / 粗回测，不应该进入主执行结果。

LOB 有缺口：
    gap 后重置 book 和 maker queue，不要跨缺口假装连续。
```

你之前那份执行模型文档的核心目标是 `L2 BookState + OrderFilled-derived FillTick + latency + queue heuristic + residual liquidity accounting`，这个目标不需要全市场完整 LOB 才能做，但需要对每一笔模拟订单标记“当时有没有足够的微观数据支持成交”。

---

# 1. 你到底需要哪些数据？

按重要程度排，至少需要这几类。

## 1.1 市场与 token 元数据

这是所有东西的底座。你需要：

```text
condition_id / market_id
slug / question / title
YES token_id
NO token_id
outcome mapping: token_id -> Yes/No
open time / close time / resolved time
winning outcome / winning token
tick_size
min_order_size
fee_rate / fee_schedule
neg_risk 标记
market active / accepting_orders 状态
```

原因是：`OrderFilled`、LOB、策略信号都不是天然按“市场问题”对齐的，它们很多时候是按 **asset_id / outcome token** 对齐的。Polymarket order book API 返回的也是 `market`、`asset_id`、`bids`、`asks`、`tick_size`、`min_order_size`、`last_trade_price` 这些 token 维度字段。([Polymarket 文档][1])

这部分可以来自 Polymarket CLOB / Gamma / PMXT `fetch_market` / 你自己的 market registry。**不要指望 PMXT LOB archive 单独解决全部 metadata**，因为 PMXT v2 archive 主要是 orderbook event stream，不是完整市场生命周期数据库。

---

## 1.2 L2 LOB 数据

这是 taker 和 maker 执行模型的核心。

你至少需要：

```text
timestamp
market / condition_id
asset_id
event_type
bids / asks
price
size
side
best_bid
best_ask
tick_size_change
```

PMXT v2 正好提供这类数据。PMXT v2 说明它的数据源是 Polymarket CLOB WebSocket `market` channel，存的是每小时一个 Parquet 文件；事件类型包括 `book`、`price_change`、`last_trade_price`、`tick_size_change`。([pmxt Archive][2])

PMXT v2 的列包括：

```text
timestamp_received
timestamp
market
event_type
asset_id
bids
asks
price
size
side
best_bid
best_ask
fee_rate_bps
transaction_hash
old_tick_size
new_tick_size
```

其中 `bids` / `asks` 只在 `book` 事件里有，`price` / `size` / `side` 在 `price_change` 和 `last_trade_price` 里出现。([pmxt Archive][2])

Polymarket 官方 market WebSocket 也说明：`book` 是 L2 orderbook snapshot，`price_change` 在新订单或取消订单时发出，`size = "0"` 表示该价格档从 book 中移除；`last_trade_price` 是撮合成交后的 trade event。([Polymarket 文档][3])

所以 PMXT 的 LOB 对你非常有用。

---

## 1.3 `OrderFilled` 链上成交数据

这部分你已经有，是你的 **settlement truth / execution evidence**。

你需要字段：

```text
block_number
block_time
tx_hash
log_index
orderHash
maker
taker
makerAssetId
takerAssetId
makerAmountFilled
takerAmountFilled
fee
```

Polymarket 文档说明，trade settled onchain 时 Exchange contract 会 emit `OrderFilled`，字段包括 `orderHash`、`maker`、`taker`、`makerAssetId`、`takerAssetId`、`makerAmountFilled`、`takerAmountFilled`、`fee`；其中 `makerAssetId == 0` 表示 maker 是 BUY，`takerAssetId == 0` 表示 maker 是 SELL。([Polymarket 文档][4])

你的模型里，`OrderFilled` 应该被规范化成：

```text
FillTick / TradeTick / ExecutionEvidence
```

例如：

```text
asset_id
price
size
passive_side: BUY/SELL
aggressor_side: BUY/SELL
ts_settle
tx_hash
log_index
orderHash
fee
```

注意：`OrderFilled` 很细，但它仍然不是 L3。它只能证明“某张 maker order 被成交了”，不能证明“没成交订单在哪里、撤单发生在哪里、你的模拟订单排在队列哪里”。

---

## 1.4 更精确的 trade timestamp，最好有 `match_time`

如果你只有链上 `block_time`，成交时间会偏晚，因为 Polymarket 是 offchain matching + onchain settlement。Polymarket 文档明确说，订单是 offchain 创建和撮合，再 onchain settlement；CLOB operator 先 match，随后 Exchange contract 结算。([Polymarket 文档][5])

Polymarket trade API 里的 trade object 有 `match_time`、`transaction_hash`、`maker_orders` 等字段。([Polymarket 文档][4])

所以优先级应该是：

```text
最优：Polymarket trade match_time / PMXT last_trade_price timestamp
其次：PMXT timestamp
最后：OrderFilled block_time
```

但实现上要保守：

```text
如果你不能可靠 join PMXT trade 和 OrderFilled，
那 maker queue 的成交推进不要提前到 block_time 之前。
```

---

## 1.5 策略订单数据和延迟模型

你还需要记录策略实际“想下什么单”：

```text
signal_ts
submit_ts
venue_received_ts = submit_ts + latency
market_id
asset_id
side
limit_price
size
time_in_force: GTC / GTD / FOK / FAK / IOC
post_only
cancel_ts
replace_ts
```

Polymarket 所有订单本质上都是 limit order；market order 只是价格设置得会立即打 resting orders 的 limit order；post-only 如果会 cross book 会被拒绝。([Polymarket 文档][5])

所以你的执行模型里不要发明“无限市价单”，应该统一成：

```text
market order = aggressive limit order
```

---

# 2. PMXT 到底包含什么？怎么用？

你要区分两个东西：

```text
PMXT SDK/API：
    给你统一接口，比如 fetch_order_book、fetch_trades、fetch_ohlcv。

PMXT Archive：
    公开的历史 orderbook Parquet 数据，是你做严肃回测更该用的东西。
```

## 2.1 PMXT Archive v2 的内容

PMXT Archive v2 是：

```text
Polymarket CLOB orderbook event stream
hourly Parquet dumps
Cloudflare R2
public HTTPS
no credentials required
```

它的官方说明是：每 UTC 小时一个文件，约在小时结束后 5 分钟写出；v2 覆盖从 `2026-04-13T19 UTC` 开始；文件 100–400 MB；数据源是 Polymarket CLOB WebSocket market channel。([pmxt Archive][2])

文件命名模式是：

```text
polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

直接下载模式类似：

```bash
curl -O https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-17T12.parquet
```

PMXT 文档也明确说，v2 的 raw bulk 下载应该用 Parquet archive，而不是用 API 去流式拉大量历史数据。([pmxt][6])

---

## 2.2 PMXT v2 的事件类型

你会用到四类：

```text
book
price_change
last_trade_price
tick_size_change
```

你应该这样映射：

```text
book:
    转成 BookSnapshot。
    重建 bids / asks。

price_change:
    转成 BookDelta。
    side=BUY 更新 bid 档。
    side=SELL 更新 ask 档。
    size=0 删除该价格档。

last_trade_price:
    转成 TradeTick 候选。
    可以辅助 maker queue 消耗。
    可以和 OrderFilled 按 tx_hash / asset_id / price / size / 时间窗口 join。

tick_size_change:
    更新 instrument tick_size。
    后续订单价格必须符合新 tick。
```

Polymarket 官方 market channel 的 `price_change` 语义是新订单放置或订单取消，`size=0` 表示价位移除；`last_trade_price` 是 maker/taker order matched 后产生的 trade event。([Polymarket 文档][3])

---

## 2.3 PMXT API 的用法

小窗口调试时，你可以用 PMXT 的 `fetch_order_book`。

PMXT 文档说明：实时查询时 `fetch_order_book(token_id)` 走 live CLOB；历史查询时，传 `since` 可以拿指定时间点之前最近的 reconstructed L2 snapshot；传 `since + until` 可以拿一段时间内 reconstructed L2 snapshots，最多 `limit=1000`，并且历史 order book 数据 backed by PMXT Archive。([pmxt][6])

所以推荐用法是：

```text
开发 / debug / 小样本：
    用 PMXT API fetch_order_book(since/until) 查 reconstructed snapshot。

正式回测 / 大规模：
    下载 Parquet raw archive。
    本地按 market / asset_id / hour 过滤。
    转成 BookSnapshot / BookDelta。
```

---

## 2.4 prediction-market-backtesting 是怎么用 PMXT 的

`prediction-market-backtesting` 的文档也基本验证了这个方向：它把 PMXT 作为 Polymarket L2 order-book raw archive path，推荐 raw-first workflow：先 mirror raw PMXT archive hours 到本地，再在 runner 里配置 `local:/...`、`archive:r2v2.pmxt.dev`、`archive:r2.pmxt.dev` 作为 source fallback。它的 loader 会把 `book_snapshot` 当 fresh full book snapshot，把 `price_change` 当 incremental price-level update；如果小时数据缺失，它会 warning 并 reset local book state，不会跨 missing-hour gap 继续套用后面的增量更新。([GitHub][7])

这点非常重要：**gap 后 reset book**，不要假装 book 连续。

---

# 3. PMXT 够不够？

我的判断：

```text
PMXT v2 对 L2 book replay 是够用的。
PMXT 单独对完整执行模型不够。
PMXT + 你的 OrderFilled，足够做严肃的 L2OrderFilledExecutionModel。
```

更具体一点：

## 3.1 对 taker 执行，PMXT 基本够

Taker 需要知道：

```text
订单到达时 best ask / best bid 是多少
每档 depth 是多少
你的 size 会吃几档
是否 partial fill
limit price 是否挡住更差价格
```

PMXT 的 `book + price_change` 可以重建 L2 BookState，所以对于 **有覆盖、有新鲜 book、无 gap 的时间段**，PMXT 足够支持 taker walk-the-book。

也就是说：

```text
BUY taker:
    用 PMXT asks 逐档吃。

SELL taker:
    用 PMXT bids 逐档吃。
```

但要加这些保护：

```text
book 太旧，不成交或 depth_haircut。
book 中间有 gap，不成交，等新 book。
只能吃可见 depth，不能扫过最后一档。
同一个 snapshot interval 内维护 residual book，防 double count。
```

---

## 3.2 对 maker 执行，PMXT 只够做 L2 queue heuristic

Maker 需要知道：

```text
我挂单时，同价位前面有多少 queue？
后续真实成交有没有消耗完前面的 queue？
未解释的 book size decrease 是前面的人撤了，还是后面的人撤了？
```

PMXT 能告诉你：

```text
同价位 visible size
price level size 变化
last_trade_price 成交事件
```

但 PMXT 不能告诉你：

```text
单个 order_id
同价位 FIFO 顺序
谁撤单
谁在你前面撤单
谁在你后面撤单
```

所以 maker 不能做真实 L3 FIFO，只能做：

```text
queue_ahead = 同侧同价位 visible size * queue_ahead_fraction
后续 OrderFilled / PMXT last_trade_price 消耗 queue_ahead
LOB decrease 未被成交解释的部分，用 cancel_ahead_fraction 保守处理
```

这就是 L2 queue heuristic。

---

## 3.3 PMXT 不能替代 `OrderFilled`

PMXT 的 `last_trade_price` 可以当 trade tick 候选，但它没有 `OrderFilled` 那么强的链上结算细节。

`OrderFilled` 有：

```text
orderHash
maker
taker
makerAssetId
takerAssetId
makerAmountFilled
takerAmountFilled
fee
tx_hash / log_index
```

PMXT 有：

```text
timestamp
asset_id
price
size
side
fee_rate_bps
transaction_hash
```

所以最佳组合是：

```text
PMXT:
    提供 L2 BookState 和更接近 match-time 的 trade event timestamp。

OrderFilled:
    提供链上 settlement truth、order-level fill、fee、maker/taker asset mapping。

合并后：
    TradeEvidence(source = both / pmxt_only / orderfilled_only)
```

优先级我建议这样：

```text
如果 PMXT last_trade_price 能和 OrderFilled 可靠 join：
    用 PMXT timestamp 作为 match_ts。
    用 OrderFilled 作为 settlement / fee / side normalization truth。

如果不能 join：
    maker queue 的主模型只用 OrderFilled block_time 推进。
    PMXT last_trade_price 单独用于数据对账和敏感性分析。
```

---

# 4. 你没法覆盖所有 market 的 LOB，应该怎么办？

不要强行全市场同一精度。你应该建立 **coverage-aware execution**。

核心原则：

```text
回测可以覆盖很多市场；
但主执行结果只能包含“成交时刻有足够微观数据支持”的订单。
```

也就是说，不是按 market 一刀切，而是按 **market + asset_id + time window** 判断。

---

## 4.1 先建一张 `lob_coverage_manifest`

每个 market / asset / hour 一行：

```text
market_id
asset_id
hour
has_pmxt_file
has_book_snapshot
has_price_changes
has_last_trade_price
first_event_ts
last_event_ts
event_count
book_event_count
price_change_count
last_trade_count
max_gap_ms
coverage_quality
```

`coverage_quality` 可以分成：

```text
HIGH:
    有 book snapshot anchor
    deltas 连续
    book_age 在 TTL 内
    没有小时 gap

MEDIUM:
    有 book，但事件较稀疏
    或只有部分时间段可用
    可以做 taker with haircut
    maker 只能 conservative

LOW:
    只有零散 quote / top-of-book / 很旧 book
    不进入主执行结果

NONE:
    没有 LOB
```

每笔策略订单执行前，先查：

```text
order.market_id
order.asset_id
order.venue_received_ts
```

得到当时的 `coverage_quality`。

---

## 4.2 主结果只跑 `HIGH/MEDIUM`，其他单独报告

我建议你的报告分成三块：

```text
Main microstructure backtest:
    只包含 LOB 覆盖足够的订单。

Coverage-excluded signals:
    策略本来想交易，但当时没有足够 LOB，所以未执行。

Coarse baseline:
    用 OHLCV / formula slippage 做粗略参考，但不能和主结果混在一起。
```

这样你不会因为缺数据而虚构成交，也不会因为丢弃缺数据订单而悄悄隐藏问题。

报告里必须输出：

```text
signals_total
orders_attempted
orders_with_high_lob
orders_with_medium_lob
orders_without_lob
notional_with_lob / total_notional
markets_with_lob / markets_total
PnL_main_microstructure
PnL_coarse_baseline
```

如果一个策略 80% 信号都没有 LOB 覆盖，那主结果只能说明“在可覆盖市场上表现如何”，不能代表全市场。

---

## 4.3 不要为了覆盖率用未来数据补当前 book

绝对不要做：

```text
当前 10:00 没 book，
但 10:05 有 snapshot，
所以拿 10:05 的 book 回填 10:00。
```

这是 look-ahead。

正确做法：

```text
当前没有 book：
    不允许 taker fill。
    maker queue 不接受新订单。
    直到新的 book snapshot 出现后，才恢复执行。
```

如果中间缺一个小时 PMXT 文件，也应该 reset book。`prediction-market-backtesting` 的 PMXT loader 就是这样处理的：缺小时会 reset local book state，不跨 missing-hour gap 应用后续 incremental `price_change`。([GitHub][7])

---

## 4.4 不需要下载所有市场，按策略窗口懒加载

你不是做数据供应商，不需要一开始下载所有 PMXT。

正确流程是：

```text
第一步：用轻数据构建候选 universe
    Gamma / market metadata / OHLCV / OrderFilled / volume / category

第二步：跑 signal，但先不成交
    记录策略在哪些 market、asset_id、时间点想下单

第三步：根据 order windows 拉 PMXT
    对每个订单拉 venue_received_ts 前后需要的小时
    例如 warmup 1 小时 + order window + settlement window

第四步：本地过滤并缓存
    按 market + asset_id + hour 保存 filtered parquet/arrow

第五步：只对 covered orders 跑微观执行
```

PMXT v2 的 Parquet 文件按 `(market, asset_id, timestamp_received)` 排序，并说明 exact match on `market` / `asset_id`、time range on `timestamp_received` 是 fast predicates。([pmxt Archive][2])

所以你应该让 Codex 做：

```text
Raw hourly parquet -> filtered cache by market/asset/hour -> BookEvent stream
```

不要每次回测都全量扫描小时文件。

---

# 5. 不同覆盖场景下，执行模型怎么降级？

这是最关键的工程规则。

## 场景 A：有 PMXT LOB + 有 OrderFilled

这是最佳情况。

执行策略：

```text
Taker:
    用 PMXT BookState walk book。
    用 residual book 防 double count。
    用 OrderFilled / PMXT last_trade_price 做事后对账。

Maker:
    初始 queue_ahead = PMXT 同侧同价位 visible size。
    OrderFilled 推进 queue。
    PMXT price_change / snapshot decrease 推断 cancel。
    conservative / realistic / optimistic 三套结果。
```

这是主回测结果。

---

## 场景 B：有 PMXT LOB，但没有你自己的 OrderFilled

这仍然可以做，但可信度下降。

执行策略：

```text
Taker:
    仍然可以用 PMXT LOB walk book。

Maker:
    可以用 PMXT last_trade_price 推进 queue，
    但要确认 side 语义和 size 聚合方式。
    结果标记为 pmxt_trade_only，不要和 OrderFilled-verified 结果混为一谈。
```

适合：

```text
早期开发
小窗口研究
和 OrderFilled 结果交叉验证
```

不适合：

```text
最终审计级成交报告
```

---

## 场景 C：有 OrderFilled，但没有 PMXT LOB

这只能做非常保守的模型。

你知道：

```text
历史上某时某价某量成交了。
```

但你不知道：

```text
你下单那一刻 book 上有没有流动性。
你 taker 可以吃多少。
你 maker 前面有多少 queue。
```

执行策略：

```text
Taker:
    主模型不成交。
    或者只做 trade-tape replay fallback：
        订单到达后，如果未来一段时间有反向成交且价格满足 limit，
        可以按很小 participation rate 成交。
    这个结果只能叫 fallback，不叫真实 taker execution。

Maker:
    主模型不成交。
    如果做敏感性分析，可以设置非常保守 queue_ahead_unknown：
        例如必须看到未来成交量 >= huge_threshold 才允许 fill。
```

我不建议把这个场景放进主 PnL。

---

## 场景 D：只有 OHLCV / price history

执行策略：

```text
只做 signal research。
不做主执行。
可以做 formula slippage baseline。
```

报告必须标记：

```text
execution_model = coarse_price_history
not_microstructure_validated
```

这类结果不能和 PMXT+OrderFilled 的结果直接比较。

---

## 场景 E：LOB 有缺口

例如：

```text
10:00 有 book
10:01-10:30 缺数据
10:31 又有 price_change
```

处理：

```text
不要把 10:31 的 price_change 应用到 10:00 的旧 book。
reset BookState。
取消或冻结所有依赖旧 book 的 maker queue。
等待下一个 book snapshot 重建。
gap 内策略订单不成交。
```

这是为了避免“增量更新接到错误的旧状态上”。

---

# 6. 你最终的数据表应该长什么样？

我建议你最终落地成这些表。

## 6.1 `markets`

```text
market_id
condition_id
slug
title
category
start_time
end_time
resolution_time
status
neg_risk
tick_size_default
min_order_size
fee_schedule
```

## 6.2 `outcomes`

```text
market_id
asset_id
outcome_name: YES / NO
is_yes
is_no
winning_asset_id
```

## 6.3 `pmxt_book_events`

```text
timestamp
timestamp_received
market_id
asset_id
event_type
bids
asks
price
size
side
best_bid
best_ask
fee_rate_bps
transaction_hash
old_tick_size
new_tick_size
source_file
```

## 6.4 `orderfilled_raw`

```text
block_number
block_time
tx_hash
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

## 6.5 `fill_ticks`

这是规范化后的核心表：

```text
fill_id
match_ts
settle_ts
market_id
asset_id
price
size
passive_side
aggressor_side
source
tx_hash
log_index
orderHash
fee
confidence
```

`source` 可以是：

```text
orderfilled_only
pmxt_last_trade_only
pmxt_orderfilled_joined
```

## 6.6 `lob_coverage_manifest`

```text
market_id
asset_id
hour
coverage_quality
has_snapshot
has_gap
max_gap_ms
event_count
book_count
price_change_count
last_trade_count
first_ts
last_ts
```

这个表非常重要。你后面的回测质量都靠它。

---

# 7. PMXT + OrderFilled 怎么合并成有效成交系统？

执行时按这个事件流：

```text
PMXT book / price_change / tick_size_change
        ↓
BookState

PMXT last_trade_price
        ↓
TradeTickCandidate

OrderFilled
        ↓
OnchainFillTick

TradeTickCandidate + OnchainFillTick
        ↓
TradeEvidence

StrategyOrder + latency
        ↓
MatchingEngine

Taker:
    BookState walk book

Maker:
    queue_ahead from BookState
    queue consumption from TradeEvidence
    cancel inference from BookState decrease
```

## 7.1 Taker 逻辑

```text
BUY:
    吃 asks，从低到高。
    只能吃 price <= limit_price。

SELL:
    吃 bids，从高到低。
    只能吃 price >= limit_price。
```

必须处理：

```text
partial fill
FOK 全量不够则 reject
FAK/IOC 成交可成交部分，剩余 cancel
GTC/GTD 剩余部分进入 maker queue
post_only crossing reject
residual book
book stale
book gap
```

PMXT 对 taker 的贡献是：

```text
给你当时 visible depth。
```

`OrderFilled` 对 taker 的贡献是：

```text
用来校验你的模拟成交量是否远超历史真实成交量。
用来做 capacity report。
不是 taker 成交的唯一触发条件。
```

因为 taker 是 counterfactual：历史没人吃，不代表你当时不能吃。

---

## 7.2 Maker 逻辑

Maker 的核心状态是：

```text
LevelQueue(asset_id, side, price):
    env_ahead
    agent_orders FIFO
```

挂单时：

```text
BUY maker @ P:
    env_ahead = PMXT bid size at P

SELL maker @ P:
    env_ahead = PMXT ask size at P
```

后续成交推进：

```text
OrderFilled passive_side=BUY, price=P:
    推进 BUY @ P queue

OrderFilled passive_side=SELL, price=P:
    推进 SELL @ P queue
```

处理顺序：

```text
先消耗 env_ahead。
env_ahead 清零后，剩余成交量才 fill 你的 agent order。
```

LOB decrease 处理：

```text
old_size - new_size = total_decrease
trade_explained = 同区间 OrderFilled / PMXT last_trade_price volume
unexplained = total_decrease - trade_explained
```

然后：

```text
conservative:
    queue_ahead -= trade_explained

realistic:
    queue_ahead -= trade_explained + unexplained * 0.3~0.5

optimistic:
    queue_ahead -= trade_explained + unexplained * 0.7~1.0
```

不要把所有 decrease 都当成交。Polymarket 官方 `price_change` 本身就可能来自新订单或取消订单，不是纯成交事件。([Polymarket 文档][3])

---

# 8. PMXT 的限制，你要在代码里显式承认

## 限制一：PMXT v2 开始时间有限

PMXT v2 overview 写的是从 `2026-04-13T19 UTC` 开始；当前 archive 页面已经有 2026-06-28 的小时文件，说明它在持续更新。([pmxt Archive][2])

如果你的回测要覆盖更早时间：

```text
要么用 PMXT v1，但 v1 数据质量要单独标记；
要么用其他 vendor；
要么这段时间不进入主微观执行结果。
```

PMXT v2 文档说 v1 有结构性问题，包括 schema 臃肿、市场覆盖缺失，v2 的目标是修复这些问题；其中 v1 曾缺大约 50% live markets，而 v2 订阅 every live asset，且冗余 exporter 让 gaps 变少。([pmxt Archive][2])

所以：**优先用 v2；v1 只能作为 lower confidence source。**

---

## 限制二：PMXT 不是 L3

PMXT 是 L2 MBP：

```text
price level size
不是 individual order queue
```

因此你不能从 PMXT 得到：

```text
order_id
订单进入时间
同价位 FIFO
谁撤单
你的订单真实排第几个
```

所以你的模型必须叫：

```text
L2OrderFilledExecutionModel
```

不要叫：

```text
L3ExecutionModel
TrueFIFOExecutionModel
```

---

## 限制三：PMXT Archive 不等于完整市场数据库

PMXT v2 archive 存的是四类 orderbook/trade/tick event，不是完整 metadata、resolution、fee lifecycle、strategy universe。你仍然要单独维护 market registry。([pmxt Archive][2])

---

# 9. 最推荐你的处理策略

我建议你直接按下面的规则落地。

## 9.1 回测时先生成订单，再拉 LOB

不要先全量拉 PMXT。

流程：

```text
1. 用轻数据跑 signal。
2. 生成 StrategyOrderIntent。
3. 收集这些订单涉及的 market_id / asset_id / hour。
4. 下载或查询对应 PMXT hour。
5. 构建 BookState。
6. 执行订单。
7. 输出 coverage report。
```

这样你只处理策略真正用到的 market/time。

---

## 9.2 主回测结果只认 `microstructure_eligible_orders`

每笔订单执行前判断：

```text
has_market_metadata
has_valid_asset_mapping
has_lob_snapshot_anchor
book_age <= TTL
no_gap_since_snapshot
price conforms to tick_size
```

满足才进入：

```text
microstructure_eligible_orders
```

不满足就进入：

```text
coverage_excluded_orders
```

不要把 excluded orders 静默丢掉。

---

## 9.3 对不同数据质量使用不同 execution policy

```text
HIGH:
    taker 正常 walk book
    maker queue_ahead 正常
    OrderFilled + PMXT reconciliation

MEDIUM:
    taker depth_haircut，例如 0.5~0.8
    maker 只用 conservative trade_only
    不使用 optimistic 作为主结果

LOW:
    不做 maker
    taker 只允许极小 size 或不成交
    单独标记

NONE:
    主模型不成交
    只进入 coarse baseline
```

---

## 9.4 三个 PnL 结果必须分开

你最后不要只输出一个 PnL。

要输出：

```text
PnL_micro_conservative
PnL_micro_realistic
PnL_micro_optimistic
PnL_coarse_baseline
PnL_excluded_due_to_missing_lob: 不存在，只统计 signals/orders
```

如果一个策略：

```text
conservative 亏
realistic 小赚
optimistic 大赚
```

那说明它对 queue 假设高度敏感。

如果一个策略：

```text
conservative 也赚钱
realistic 更好
optimistic 更好
```

才说明执行层更稳。

---

# 10. 最终回答：PMXT 够么？

**够一大半。**

更准确地说：

```text
PMXT 足够承担 L2 LOB 数据源角色。
PMXT 不够承担完整 execution truth。
PMXT + OrderFilled 才是你当前最合理的数据组合。
```

你的最低可用组合应该是：

```text
market metadata
+ PMXT v2 LOB events
+ OrderFilled normalized FillTicks
+ block_time / optional match_time
+ resolution data
+ fee/tick/min-size metadata
```

用这套，你可以做：

```text
taker walk-the-book
partial fill
FOK / FAK / GTC / post-only
residual book
maker queue_ahead
OrderFilled queue consumption
LOB decrease reconciliation
coverage-aware execution
```

但你不能做：

```text
真实 L3 FIFO
精确撤单归属
所有历史市场全时期统一精度执行
无覆盖市场的微观成交模拟
```

所以你的 Codex 实现应该围绕这句话展开：

```text
PMXT 负责“当时盘口”。
OrderFilled 负责“链上成交证据”。
二者合并成 TradeEvidence + BookState。
每笔订单根据 coverage_quality 决定能否进入微观执行。
缺 LOB 时不要虚构成交，只做 baseline 或排除。
```

这是最诚实、也最容易上线迭代的方案。

[1]: https://docs.polymarket.com/api-reference/market-data/get-order-book "Get order book - Polymarket Documentation"
[2]: https://archive.pmxt.dev/docs/v2-data-overview "Polymarket Orderbook Archive (v2) — Data Overview"
[3]: https://docs.polymarket.com/market-data/websocket/market-channel "Market Channel - Polymarket Documentation"
[4]: https://docs.polymarket.com/trading/orders/overview "Overview - Polymarket Documentation"
[5]: https://docs.polymarket.com/concepts/order-lifecycle "Order Lifecycle - Polymarket Documentation"
[6]: https://www.pmxt.dev/docs/api-reference/fetch-order-book "Fetch Order Book - PMXT"
[7]: https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/v4.1-alpha/docs/data-vendors.md "raw.githubusercontent.com"
