对于**仅有 fill trade / OrderFilled trade tape** 的成交模型，你要先把“准不准”定义清楚：

**它不能验证“我当时真实盘口上一定能成交”。**
因为没有 LOB，你不知道当时 best bid/ask、depth、spread、queue、撤单和未成交挂单。它能验证的是：

```text
我的模拟订单是否只使用了真实发生过的成交证据；
是否没有用未来数据；
是否没有用错方向；
是否没有突破 limit；
是否没有超过历史成交容量；
是否每笔模拟 fill 都能追溯到具体 trade_id / tx_hash / log_index。
```

所以它的准确性不是 **full depth accuracy**，而是 **fill-evidence constrained accuracy**。这和你之前给这个模型的定位是一致的：OrderFilled-only 不是 L2/L3/True FIFO，而是基于真实成交流的保守参与模型。

---

## 1. 先验证数据层准不准

这是第一层。如果数据处理错了，后面的成交模型全错。

你要验证这条链：

```text
raw_orderfilled
    ↓
maker_fill_ticks
    ↓
trade_prints_one_sided
    ↓
block_trade_bars_sparse / time_bars
    ↓
orderfilled_only_execution_engine
```

执行层必须只读 `trade_prints_one_sided`，不能读 raw OrderFilled、maker_fill_ticks、block OHLCV 或 forward-filled mark。OHLCV 只能做信号、估值、画图和统计，不能驱动成交。

数据层要检查：

```text
raw_orderfilled_count
maker_fill_ticks_count
trade_prints_one_sided_count
quarantine_count
invalid_price_count
invalid_size_count
duplicate_tx_log_orderhash_count
missing_timestamp_count
unsupported_asset_path_count
```

硬性验收：

```text
price 必须在 (0, 1]
size 必须 > 0
market_id / condition_id / asset_id 不为空
side 映射必须正确
quarantine 行不能进入执行层
one-sided volume 不能双算
trade_prints_one_sided 必须保留 source_orderfilled_ids
```

尤其要验证这两个方向映射：

```text
makerAssetId == 0:
    passive_side = BUY
    aggressor_side = SELL

takerAssetId == 0:
    passive_side = SELL
    aggressor_side = BUY
```

如果这一步不过，后面所有“准不准”都没有意义。

---

## 2. 再验证成交规则准不准

OrderFilled-only taker 的主规则应该是：

```text
BUY taker:
    只能使用 arrival_ts 之后
    aggressor_side = BUY
    price <= limit_price
    deadline 之前
    每条 trade 最多分配 participation_rate * trade_size

SELL taker:
    只能使用 arrival_ts 之后
    aggressor_side = SELL
    price >= limit_price
    deadline 之前
    每条 trade 最多分配 participation_rate * trade_size
```

没有合格 future trade print，就不能成交。

### 必须做的 golden tests

构造小型人工数据，逐条验证。

**BUY 方向测试**

```text
trade_1: BUY  @ 0.52 size=1000
trade_2: SELL @ 0.51 size=1000

order:
BUY limit=0.53 size=100 participation=10%
```

期望：

```text
只能用 trade_1
不能用 trade_2
最多 fill 100
```

**SELL 方向测试**

```text
trade_1: BUY  @ 0.52 size=1000
trade_2: SELL @ 0.51 size=1000

order:
SELL limit=0.50 size=100 participation=10%
```

期望：

```text
只能用 trade_2
不能用 trade_1
最多 fill 100
```

**limit 测试**

```text
BUY limit=0.53
future trades:
BUY @ 0.52
BUY @ 0.55
```

期望：

```text
只能用 0.52
不能用 0.55
```

**latency / no-lookahead 测试**

```text
trade_1: t=10 BUY @ 0.52
trade_2: t=12 BUY @ 0.52

order:
signal_ts = t=9
latency = 2
arrival_ts = t=11
```

期望：

```text
不能用 t=10
只能用 t=12
```

**capacity ledger 测试**

```text
trade_id=A
BUY @ 0.52 size=1000
participation_rate=10%
capacity=100

order_1 BUY size=80
order_2 BUY size=80
```

期望：

```text
order_1 fill 80
order_2 最多 fill 20
两者合计不能超过 100
```

这一个非常关键。没有共享 capacity ledger，回测会严重高估成交量。

---

## 3. 验证“不变量”是否永远成立

每次跑完一个回测 run，都要自动检查：

```text
fill.size > 0
order.filled_size <= order.original_size
fill.ts >= order.arrival_ts
BUY fill.price <= order.limit_price
SELL fill.price >= order.limit_price
BUY fill 只能来自 aggressor_side=BUY
SELL fill 只能来自 aggressor_side=SELL
trade_id 被消费容量 <= trade_size * participation_rate
无 future trade print 时不能成交
maker strict 模式不能产生 fill
所有 fill 都有 source trade_id / tx_hash / log_index
同样输入重复运行，输出 hash 完全一致
```

验收标准应该是：

```text
wrong_side_violations = 0
limit_violations = 0
lookahead_violations = 0
capacity_overuse = 0
missing_source_trade = 0
determinism_diff = 0
```

这部分不是“建议”，而是 CI gate。任何一项不为 0，这个 run 就不能算 valid。

---

## 4. 用 reference engine 对照 optimized engine

你现在已经遇到过性能问题，所以应该保留两个版本：

```text
SlowReferenceMatcher:
    规则最简单，慢，但可读、可审计。

Indexed/OptimizedMatcher:
    用分组、二分、capacity array 加速。
```

在小样本和中样本上强制比较：

```text
fills 完全一致
capacity ledger 完全一致
order final state 完全一致
unfilled reason 完全一致
```

测试规模建议：

```text
small:
    100 trades / 20 orders

medium:
    100,000 trades / 10,000 orders

randomized:
    固定 seed 随机生成 100 组 trade tape + orders
```

如果 optimized matcher 和 reference matcher 结果不一致，先不要看性能，先修正确性。

---

## 5. 用真实钱包 / 真实成交 replay 验证 plumbing

这个验证不是为了证明 counterfactual 一定准，而是为了证明你的数据和成交方向没错。

做法：

从真实 `trade_prints_one_sided` 抽样一些真实成交：

```text
tx_hash
asset_id
aggressor_side
price
size
block_number
log_index
```

构造一个测试订单：

```text
arrival_ts = trade_ts - epsilon
side = aggressor_side
limit_price = trade_price + buffer for BUY
limit_price = trade_price - buffer for SELL
size = trade_size * test_participation
```

跑两种模式：

### Plumbing mode

```text
participation_rate = 100%
price_buffer = 0
latency = 0
```

期望：

```text
能复现对应 trade print 的方向、价格、size 上限、source_trade_id。
```

这个模式不是生产模型，只是检查：

```text
side mapping 是否正确
price 是否正确
tx/log/source 是否正确
engine 是否能消费对应成交
```

### Production mode

```text
participation_rate = 0.5% / 1% / 2.5%
price_buffer = 1 tick
latency > 0
```

期望：

```text
只能成交真实成交的一小部分
不能超过 capacity
不能用错方向
不能用未来之前的成交
```

---

## 6. 用 LOB 覆盖好的样本做外部对照

虽然你问的是“仅有 fill trade 模型”，但如果你现在已经有一些 PMXT L2 覆盖好的样本，可以把它们当成外部 holdout 来检查 OrderFilled-only 模型是否过度乐观。

做法：

对同一批订单同时跑：

```text
A. OrderFilled-only 模型
B. L2 + fill trade 模型
```

比较：

```text
OrderFilled-only 成交是否被 L2 盘口支持？
OrderFilled-only 是否经常在 L2 显示无可成交盘口时给出 fill？
OrderFilled-only 的成交价是否比 L2 book-walk 过于乐观？
OrderFilled-only 的 fill size 是否显著大于 L2 可见 depth 支持的 size？
```

合理预期：

```text
OrderFilled-only 应该更保守，或者至少不应该明显比 L2 模型更乐观。
```

如果出现：

```text
OrderFilled-only fill rate 远高于 L2 depth model
OrderFilled-only avg price 显著优于 L2 book-walk
OrderFilled-only 在 L2 stale/gap 时仍大量 fill
```

说明你的 participation_rate、horizon 或 price_buffer 太乐观。

LOB 外部对照的本质是：用 snapshot + delta 重建出的 order book 作为更高保真参照。订单簿数据的正确使用方式是 snapshot 做基线、change/delta 做增量补丁，gap 后必须 reset，不能在错误账本上继续模拟。

---

## 7. 做参数敏感性验证

OrderFilled-only 模型最核心的参数是：

```text
participation_rate
latency
execution_horizon
price_buffer
maker_phantom_queue
```

你要固定输出 sensitivity curve。

### participation curve

```text
0.5%
1%
2.5%
5%
10%
```

观察：

```text
fill_rate
filled_size
capacity_utilization
avg_fill_delay
avg_price
orders_unfilled_due_to_capacity
```

如果策略只有在 10% participation 下才表现很好，那说明它依赖过高历史成交占比，不应作为主结果。

### latency curve

```text
0ms
100ms
500ms
1000ms
5000ms
```

观察：

```text
fill_rate 是否急剧下降
avg_price 是否变差
错过成交的订单占比
```

如果 0ms 很好、500ms 就崩，说明策略极度依赖不现实的反应速度。

### horizon curve

```text
5s
30s
5m
15m
1h
```

如果 horizon 越长，fill 越多，这是正常的；但你要解释：

```text
这到底是“订单会慢慢成交”，还是“模型拿很久之后的无关成交流给自己补成交”。
```

主结果不要用过长 horizon，除非策略真实订单就是 GTC/GTD 并且你明确建模了订单存活。

---

## 8. 验证 maker 功能是否被正确限制

仅 fill trade 模型下，maker 很危险。没有 LOB 你不知道 queue，所以主模型建议：

```text
maker strict:
    不成交，只记录 candidate
```

如果做 phantom queue sensitivity，也只能作为上界/敏感性分析。

需要测试：

```text
maker strict 模式 fill_count = 0
BUY maker 只能被 aggressor_side=SELL 且 price <= order.limit_price 的 trade 推进
SELL maker 只能被 aggressor_side=BUY 且 price >= order.limit_price 的 trade 推进
phantom_queue 必须先被消耗，清零后才能 fill
maker fill size 不能超过 participation cap
```

报告里要明确区分：

```text
taker fills
maker strict candidate fills
maker phantom sensitivity fills
```

不要把 maker optimistic 结果混进主结果。

---

## 9. 验证 block/time OHLCV 没被误用

你应该专门写一个“反作弊”测试。

构造一个 block：

```text
block 1000:
BUY  @ 0.70 size=5
SELL @ 0.55 size=9995
```

block OHLCV 是：

```text
high = 0.70
volume = 10000
```

策略：

```text
BUY size=1000 limit=0.70
```

如果执行层错误使用 OHLCV，可能会成交很多。
正确结果：

```text
最多只能参与 BUY @ 0.70 那 5 shares 的 participation cap。
```

再构造无成交 block：

```text
block 1001 no trade
forward-fill close = 0.55
```

策略：

```text
BUY @ 0.55
```

正确结果：

```text
无 trade print，不成交。
```

这能保证：

```text
block_marks_dense
time_bars
forward-filled close
```

都没有被执行引擎误用。

---

## 10. 验证报告是否可审计

“准”之外，OrderFilled-only 最重要的价值是可审计。

每一笔模拟 fill 必须能回答：

```text
为什么成交？
用了哪条 trade print？
source tx_hash 是什么？
source log_index 是什么？
source_orderfilled_ids 是什么？
历史成交方向是什么？
历史成交价格是多少？
历史成交 size 是多少？
participation cap 是多少？
我的订单 arrival_ts 是多少？
有没有价格 buffer？
有没有用未来数据？
剩余未成交原因是什么？
```

每个 fill audit 至少包含：

```text
order_id
fill_id
model_profile
arrival_ts
deadline_ts
side
limit_price
requested_size
filled_size
exec_price
source_trade_id
source_tx_hash
source_log_indexes
historical_trade_price
historical_trade_size
historical_aggressor_side
participation_rate
trade_capacity_before
trade_capacity_after
unfilled_reason
```

如果 fill 没有 source trade，就不能进入正式结果。

---

## 11. 性能和扩展性也要验证

除了准不准，还必须验证快不快。

因为 fill trade 模型很容易写成：

```text
for order in orders:
    scan all trades
```

这会变成：

```text
O(orders * trades)
```

你要验证：

```text
是否按 (market_id, asset_id, aggressor_side) 分组
是否每个 order 用二分定位 arrival/deadline
是否只扫描 candidate trades
是否没有 per-order SQL
是否 capacity ledger 是共享数组/索引，不是每个 order 独立复制
```

性能报告要输出：

```text
orders_count
trade_prints_loaded
candidate_trades_scanned
candidate_rows_per_order_p50
candidate_rows_per_order_p95
candidate_rows_per_order_p99
matching_sec
orders_per_sec
capacity_updates_per_sec
peak_memory_mb
db_load_sec
persist_sec
```

验收：

```text
同样规模下 optimized matcher 与 reference matcher 结果一致
matching time 接近线性扩展
无 OOM
无 per-order full scan
```

---

## 12. 稳健性和泛化测试

不要只在一个 market、一个类别、一个时间段上验证。

至少按这些 regime 分组：

```text
高成交市场
低成交市场
高价格 0.8-0.99
低价格 0.01-0.2
中间价格 0.4-0.6
临近 resolution
远离 resolution
crypto
sports
politics
wide spread proxy
成交稀疏 market
成交密集 market
```

每组输出：

```text
fill_rate
partial_fill_rate
avg_fill_delay
capacity_utilization
unfilled_due_to_no_trade
unfilled_due_to_limit
unfilled_due_to_capacity
avg_price_buffer
```

如果模型在高流动 market 很好，但低流动 market 出现大量奇怪 fill，就要调低 participation 或缩短 horizon。

---

## 13. 数据污染和 ex-self 验证

如果你后面接 live/paper 小单，一定要验证：

```text
自己的真实订单产生的 OrderFilled 不会被拿来证明模型预测正确。
```

需要 self-trade filter：

```text
wallet
order_hash
client_order_id
tx_hash
maker/taker address
```

验证报告要输出：

```text
self_trade_removed_count
tape_including_self_result
tape_excluding_self_result
difference
```

真正用于校准的是：

```text
tape_excluding_self
```

否则会变成“我自己制造了未来成交，然后模型说预测到了成交”。

---

## 14. 你最终应该有的验证脚本

建议做这些脚本：

```text
validate_orderfilled_data_contract.py
    验证 raw -> maker_fill_ticks -> trade_prints_one_sided

validate_fill_trade_execution_invariants.py
    验证 wrong side / limit / lookahead / capacity / source event

compare_fill_trade_reference_vs_indexed.py
    慢版本和快版本逐笔对齐

run_fill_trade_golden_fixtures.py
    跑人工构造的方向、limit、latency、capacity 测试

run_wallet_replay_validation.py
    用真实钱包/真实成交做 plumbing replay

run_fill_trade_lob_holdout_validation.py
    在 LOB 覆盖市场上，对比 fill-only vs L2-depth 模型

benchmark_fill_trade_replay.py
    性能测试

generate_fill_trade_validation_report.py
    汇总所有验证结果
```

---

## 15. 最小验收标准

你可以把 V2 的验收标准写成这样。

### 数据验收

```text
invalid_price_rows = 0
invalid_size_rows = 0
duplicate_trade_id = 0
quarantine rows 不进入执行层
trade_prints_one_sided 有 source_orderfilled_ids
OHLCV 不进入执行层
```

### 规则验收

```text
wrong_side = 0
limit_violations = 0
lookahead = 0
capacity_overuse = 0
missing_source_trade = 0
determinism_diff = 0
```

### 模型验收

```text
participation curve 输出完整
latency curve 输出完整
horizon curve 输出完整
strict / conservative / optimistic 可解释
maker strict 不产生主结果 fill
unsupported strategy 被明确标记
```

### 性能验收

```text
reference vs optimized diff = 0
matching hot path 不做全量 per-order scan
candidate rows per order 有统计
大样本不 OOM
性能回归不超过 baseline 20%
```

### 审计验收

```text
每笔 fill 有 source trade_id / tx_hash / log_index
每笔 order 有 unfilled_reason
每个 run 有 config_hash / data_version / code_version
同输入同配置输出一致
```

---

## 16. 一句话总结

仅有 fill trade 的成交模型，不能证明“我当时真实盘口一定会成交”。
它能验证的是：

```text
我的订单是否严格、保守、可审计地参与了历史真实成交流。
```

所以你要验证的不只是“PnL 像不像”，而是：

```text
数据是否正确；
方向是否正确；
价格限制是否正确；
时间是否没有未来函数；
成交容量是否没有超用；
OHLCV 是否没有被误用；
每笔 fill 是否能追溯；
参数是否稳健；
性能是否能跑大样本；
在 LOB-covered 样本上是否不比 depth model 更乐观；
未来 live 校准时是否排除自己的成交污染。
```

如果这些都过了，你就可以说：

**这个 fill-trade-only 回测模型在它声明的边界内是准确的、保守的、可审计的。**
