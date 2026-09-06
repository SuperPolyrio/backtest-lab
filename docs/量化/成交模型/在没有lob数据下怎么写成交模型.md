结论先说直白一点：

**在 LOB 不齐全时，仅靠 `OrderFilled` 不可能做出“完整微观结构意义上十分准确”的回测。**
但可以做出一个**非常严肃、漂亮、可审计、可解释、保守不虚胖**的回测框架。关键是你要把问题从：

```text
我能否模拟当时完整盘口成交？
```

改成：

```text
我的策略订单是否能够合理参与历史上真实发生过的成交流？
```

这类模型我建议叫：

```text
OrderFilled-Only Trade-Tape Execution Model
或者
OrderFilled Replay Execution Model
或者
Fill-Evidence Execution Model
```

不要叫：

```text
L2 Depth Execution
L3 FIFO Execution
True Queue Execution
```

因为没有 LOB，你不知道当时 bid/ask、depth、spread、queue、撤单、未成交挂单。你只有**成交后的证据**。这份边界必须写进框架和报告里。

---

# 1. 别人的框架给出的核心启发

传统金融和预测市场框架基本分成两条路线。

第一条是**严肃微观结构路线**：需要 orderbook + trades。HftBacktest 明确说它是 market-data replay，订单不能改变回放市场，因此核心假设是你的订单足够小、没有市场冲击；它还提醒即便有 partial fill 模型，taking liquidity 的某些 partial fill 仍可能不现实，因为回放数据不会因为你的订单而改变。([HftBacktest][1]) NautilusTrader 的 backtest 文档也强调 trade ticks 可以触发 opposite-side 的 resting limit fill：seller trade 可以填 buy limit，buyer trade 可以填 sell limit；并且建议 book/quote 数据建立 spread baseline，trade ticks 提供 execution evidence。([NautilusTrader][2]) PredictionMarketBench 也是同一个方向：用 orderbook updates、trade prints、settlement events 做 deterministic event-driven replay，并且显式支持 maker/taker semantics 和 fee modeling。([arXiv][3])

第二条是**缺少盘口时的 volume/slippage 路线**：Zipline 的 `VolumeShareSlippage` 用历史 bar volume 的固定比例限制每个 bar 能成交的量，默认最大成交量是当前 bar 历史 volume 的 2.5%，并用 `volume_share² * price_impact` 模拟价格冲击。([Zipline][4]) QuantConnect 的 `VolumeShareSlippageModel` 也是用订单量占 bar volume 的比例平方来计算滑点，并把 `volumeLimit` 默认设为 0.025。([量化伙伴][5]) Backtrader 的 volume fillers 也提供了按 bar volume 的固定比例填单、固定最大量、按 high-low 区间分配 volume 等模型。([Backtrader][6])

预测市场里的轻量框架也会走这条路。比如 `distank/polymarket-backtest` 明确说 Polymarket 没有完整历史 orderbook，所以用 spread、slippage、commission 公式近似成交价格：BUY 用 `price + spread/2 + slippage`，SELL 用 `price - spread/2 - slippage`。([GitHub][7]) 这类模型可以很快、很漂亮，但它不是微观结构 truth。

你的情况更特殊：你没有完整 LOB，但有 `OrderFilled`。这比普通 OHLCV 强得多，因为它不是 bar volume，而是链上成交证据。Polymarket 的研究论文也说明，Polymarket 是 offchain CLOB matching + onchain settlement，`OrderFilled` 记录 fill-level execution details，包括 maker/taker、token IDs、quantities 等。([arXiv][8]) 所以你应该做的不是“OHLCV + 公式滑点”，而是“**OrderFilled trade tape + volume participation + limit/side constraints + latency + conservative capacity**”。

---

# 2. 先定义：OrderFilled-only 能准确回答什么，不能回答什么

`OrderFilled` 能准确回答：

```text
历史上某个 market/token 在某个时间附近，真的发生了哪些成交？
成交方向是什么？
价格是多少？
数量是多少？
maker/taker 谁参与？
成交是否最终 onchain settled？
```

`OrderFilled` 不能准确回答：

```text
我提交订单那一刻 best bid / best ask 是多少？
盘口上有多少 visible depth？
我的 taker 单能不能立刻吃到某一档？
我的 maker 单前面有多少 queue？
同价位谁排在我前面？
哪些订单撤掉了？
哪些订单挂着但没成交？
```

所以你要把“准确性”定义成：

```text
在没有盘口数据时，订单只能从真实历史成交流里获得成交资格；
成交量不能超过历史成交量的一小部分；
价格不能优于历史成交价格；
成交必须满足方向、价格、时间、延迟、容量约束；
所有成交都能追溯到具体 OrderFilled / tx_hash / log_index。
```

这个模型不是最乐观的，但它最诚实。

你之前文档里的原则是“不要把 `OrderFilled` 假装成 L3，而是把它做成 `FillTick / TradeTick / execution evidence`”，这个原则在 LOB 缺失时仍然成立。

---

# 3. 最核心设计：把 OrderFilled 变成一条“可消耗的历史成交流”

你需要先把原始 `OrderFilled` 变成标准化 `TradePrint`。

但是这里有一个很重要的坑：**不能简单把所有 `OrderFilled` 相加。** Paradigm 的 Polymarket volume 分析指出，Polymarket 每个交易 transaction 里通常有 maker-focused `OrderFilled`、taker-focused `OrderFilled` 和 `OrdersMatched`，直接 summing `OrderFilled` 会把 volume 双算；正确方式应该用 one-sided volume，比如 maker-side 或 taker-side，而不是把两边加起来。([Paradigm][9])

所以你的数据层应该分两张表：

```text
maker_fill_ticks:
    每个 maker order 被填了多少
    适合分析被动成交、maker 侧成交分布、地址行为

trade_prints_one_sided:
    每个 tx / matched group 的一侧成交量
    适合做回测容量、volume participation、market tape
```

`trade_prints_one_sided` 至少要有：

```text
market_id
asset_id
ts
block_number
tx_hash
trade_group_id
price
size
aggressor_side      # BUY / SELL
passive_side        # SELL / BUY
source_orderfilled_ids
fee
confidence
```

方向规则：

```text
aggressor_side = BUY:
    历史上有人主动买，吃了 ask-side liquidity。
    这可以作为“模拟 BUY taker 也许能参与该买方成交流”的证据。
    也可以作为“SELL maker 可能被成交”的证据。

aggressor_side = SELL:
    历史上有人主动卖，吃了 bid-side liquidity。
    这可以作为“模拟 SELL taker 也许能参与该卖方成交流”的证据。
    也可以作为“BUY maker 可能被成交”的证据。
```

---

# 4. OrderFilled-only 下，Taker 应该怎么设计

没有 LOB 时，**taker 不能默认“立即成交”**。因为你不知道当时 ask/bid 上有没有量。

所以 OrderFilled-only taker 的核心规则应该是：

```text
策略发出订单后，经过 latency。
从 arrival_ts 开始，向未来扫描真实历史 trade prints。
只有出现同方向、价格不差于 limit 的真实成交，才允许模拟订单参与。
每个历史成交只能给模拟订单分配一小部分容量。
```

## 4.1 BUY taker

策略订单：

```text
BUY YES
limit_price = 0.53
size = 1000
arrival_ts = signal_ts + latency
execution_horizon = 5min
participation_rate = 2.5%
```

历史成交流：

```text
10:00:01 aggressor_side=BUY  price=0.52 size=2000
10:00:20 aggressor_side=BUY  price=0.53 size=1000
10:01:10 aggressor_side=BUY  price=0.55 size=3000
10:02:00 aggressor_side=SELL price=0.51 size=4000
```

BUY taker 只能参与：

```text
aggressor_side=BUY
price <= limit_price
ts >= arrival_ts
ts <= arrival_ts + horizon
```

所以可用成交是：

```text
10:00:01 BUY @ 0.52 size=2000
10:00:20 BUY @ 0.53 size=1000
```

不能用：

```text
10:01:10 BUY @ 0.55     # 超过 limit
10:02:00 SELL @ 0.51    # 方向不对
```

每笔历史成交可分配量：

```text
capacity = historical_trade_size * participation_rate
```

如果 `participation_rate = 2.5%`：

```text
10:00:01 capacity = 2000 * 2.5% = 50
10:00:20 capacity = 1000 * 2.5% = 25
```

所以这个订单最多成交：

```text
75 / 1000
```

剩余 925 不能凭空成交。

这个思想和 Zipline / QuantConnect 的 volume share 模型一致：不是只要价格出现就无限成交，而是用历史 volume 的固定比例限制 fill capacity。Zipline 的默认 `VolumeShareSlippage` 就是用 bar volume 的 2.5% 作为默认成交上限，并用平方项计算 impact。([Zipline][4]) QuantConnect 文档也明确把 `volumeLimit` 默认设为 0.025，价格冲击由 order quantity / bar volume 的比例决定。([量化伙伴][5])

## 4.2 SELL taker

策略订单：

```text
SELL YES
limit_price = 0.48
size = 500
```

SELL taker 只能参与：

```text
aggressor_side=SELL
price >= limit_price
```

因为你卖出时，价格低于 limit 就不能成交。

历史成交：

```text
10:00:01 SELL @ 0.49 size=1000
10:00:30 SELL @ 0.48 size=500
10:01:00 SELL @ 0.46 size=3000
10:01:20 BUY  @ 0.50 size=2000
```

可用：

```text
SELL @ 0.49
SELL @ 0.48
```

不可用：

```text
SELL @ 0.46   # 低于 limit
BUY  @ 0.50   # 方向不对
```

---

# 5. OrderFilled-only 下，Maker 应该非常谨慎

没有 LOB 时，maker 最容易被高估。

因为 maker 需要知道：

```text
我挂单时前面有多少 queue？
后续卖方/买方成交有没有轮到我？
前面的人有没有撤单？
```

这些 `OrderFilled` 都不知道。

所以我建议你的主模型里：

```text
OrderFilled-only 主结果：默认不支持 maker alpha / spread capture 的真实成交。
```

但是你可以提供一个**降级 maker 模型**，只作为保守敏感性分析。

## 5.1 Maker BUY 降级模型

策略挂单：

```text
BUY YES @ 0.45
size = 100
arrival_ts = 10:00:00
```

没有 LOB，你不知道 0.45 前面有多少 bid queue。
所以定义一个“虚拟前方队列”：

```text
phantom_queue = max(
    fixed_queue_floor,
    queue_volume_multiplier * trailing_opposite_flow_at_price,
    queue_notional_floor / price
)
```

例如：

```text
fixed_queue_floor = 1000 shares
queue_volume_multiplier = 2.0
```

后续只有当历史成交流里出现：

```text
aggressor_side = SELL
price <= 0.45   # seller 打到你的 bid 或更低
```

才消耗 phantom_queue。

例子：

```text
phantom_queue = 1000
your_order = BUY 100 @ 0.45

10:01 SELL @ 0.45 size=300
10:02 SELL @ 0.45 size=500
10:03 SELL @ 0.45 size=400
```

处理：

```text
10:01:
    phantom_queue 1000 -> 700
    your_fill = 0

10:02:
    phantom_queue 700 -> 200
    your_fill = 0

10:03:
    400 先消耗 phantom_queue 200
    剩余 200 可以填你的订单
    your_fill = min(100, 200 * participation_or_maker_fill_cap)
```

如果你希望更保守：

```text
queue must be fully cleared, then fill only participation_rate * remaining_trade_volume
```

如果你希望严格：

```text
OrderFilled-only maker = no fill
```

我建议主报告用：

```text
maker_strict = no fill
maker_conservative = phantom_queue + participation cap
maker_optimistic = lower phantom_queue
```

不要只给一个漂亮结果。

## 5.2 Maker SELL 降级模型

SELL maker：

```text
SELL YES @ 0.55
```

只能被：

```text
aggressor_side = BUY
price >= 0.55
```

的历史成交推进。

同样需要 phantom_queue。

---

# 6. 成交价格怎么定：永远不要给自己“更好价格”

没有 LOB 时，最容易偷出来的利润是“价格改善”。你应该采用 worse-price rule。

## 6.1 Taker 价格

BUY taker：

```text
base_price = historical_trade_price
impact_buffer = max(tick_size, price_impact_model(...))
exec_price = base_price + impact_buffer
必须 exec_price <= limit_price
```

SELL taker：

```text
exec_price = base_price - impact_buffer
必须 exec_price >= limit_price
```

`impact_buffer` 可以先简单设：

```text
1 tick
或者 0.5c / 1c
或者 price_impact * (filled_qty / eligible_volume)^2
```

Zipline 和 QuantConnect 的 volume share 模型给了一个很好的传统金融参照：价格冲击不是线性的，而是和成交量占历史 volume 的比例相关，Zipline 代码里用 `volume_share ** 2 * price_impact * price`。([Zipline][4]) QuantConnect 的 market impact 文档也列出 execution time、volatility、liquidity、order size 是影响 slippage/market impact 的关键因素。([量化伙伴][5])

## 6.2 Maker 价格

Maker fill 不给价格改善：

```text
BUY maker:
    exec_price = order.limit_price

SELL maker:
    exec_price = order.limit_price
```

NautilusTrader 文档里也有类似保守原则：trade tick 触发 resting limit 时，fill 发生在订单 limit price，而不是给更好的 trade price。([NautilusTrader][2])

---

# 7. 订单时间和 look-ahead 控制

OrderFilled-only 模型特别容易犯 look-ahead 错误。必须强制：

```text
策略只能看到 signal_ts 之前的数据。
订单只能在 arrival_ts 之后参与成交。
arrival_ts = signal_ts + latency。
```

如果你只有链上 `block_time`，那 `OrderFilled` 只能在 `block_time` 之后使用。Polymarket 的架构是 offchain matching、onchain settlement，论文也说明完成的 match 会被提交到 Polygon 合约结算并 emit logs。([arXiv][8]) 如果你没有可靠 match_time，就不能把链上 settlement event 提前当成 CLOB match event。

建议默认：

```text
latency_ms:
    conservative: 1000ms - 5000ms
    realistic:    200ms - 1000ms
    optimistic:   0ms - 200ms

execution_horizon:
    taker: 5s / 30s / 5m
    maker: 5m / 15m / 1h，但必须单独报告
```

---

# 8. 你应该有三个 OrderFilled-only 模式

不要只做一个模型。做三套。

## 模式 A：Strict / Audit Mode

这是最保守、最漂亮、最可审计的主结果。

```text
Taker:
    只允许参与 arrival_ts 之后的同方向真实成交。
    participation_rate 很低，例如 0.5% - 2.5%。
    价格加 1 tick 或更大 buffer。
    没有真实成交就不成交。

Maker:
    默认不成交。
    或只记录 candidate fills，不计入主 PnL。
```

这个模式会让 PnL 不那么好看，但可信。

## 模式 B：Conservative Trade-Tape Mode

```text
Taker:
    参与同方向真实成交。
    participation_rate = 2.5% - 5%。
    加 impact buffer。
    允许多个 trade prints 慢慢填满。

Maker:
    使用 phantom_queue。
    queue 清零后才允许 fill。
    participation cap 仍然生效。
```

这个适合作为主研究结果。

## 模式 C：Optimistic Sensitivity Mode

```text
Taker:
    participation_rate = 5% - 10%。
    impact buffer 小。
    execution horizon 更长。

Maker:
    phantom_queue 更小。
    可以允许 price-through fills。
```

这个只能作为上界。
如果策略只有 Optimistic 能赚钱，那不是好策略。

---

# 9. 这个模型怎么让回测“漂亮”

“漂亮”不是让 PnL 变漂亮，而是让报告变漂亮、可信、可解释。

每笔模拟成交都应该能回答：

```text
为什么成交？
对应哪条 OrderFilled？
tx_hash 是什么？
历史成交方向是什么？
历史成交价格是多少？
历史成交量是多少？
我的 participation cap 是多少？
我的成交价格为什么是这个？
我的 latency 是多少？
我有没有用未来数据？
```

一笔 fill 的 audit log 应该长这样：

```json
{
  "order_id": "sim_123",
  "model": "orderfilled_trade_tape_conservative",
  "signal_ts": "2026-03-23T20:00:00Z",
  "arrival_ts": "2026-03-23T20:00:01Z",
  "side": "BUY",
  "limit_price": "0.53",
  "requested_size": "1000",
  "filled_size": "75",
  "avg_price": "0.5300",
  "source_fills": [
    {
      "tx_hash": "0xabc",
      "ts": "2026-03-23T20:00:02Z",
      "historical_aggressor_side": "BUY",
      "historical_price": "0.52",
      "historical_size": "2000",
      "participation_rate": "0.025",
      "allocated_size": "50",
      "exec_price": "0.53"
    },
    {
      "tx_hash": "0xdef",
      "ts": "2026-03-23T20:00:20Z",
      "historical_aggressor_side": "BUY",
      "historical_price": "0.53",
      "historical_size": "1000",
      "participation_rate": "0.025",
      "allocated_size": "25",
      "exec_price": "0.53"
    }
  ],
  "unfilled_size": "925",
  "reason_unfilled": "insufficient_post_arrival_same_side_orderfilled_capacity"
}
```

这就是漂亮：**不是装作准确，而是每一笔都能审计。**

---

# 10. 具体执行算法

## 10.1 标准化成交流

```python
raw_orderfilled
    -> maker_fill_ticks
    -> trade_prints_one_sided
```

要求：

```text
1. 不双算 OrderFilled。
2. 保留 tx_hash/log_index/orderHash。
3. 保留 maker-side 明细和 one-sided market tape。
4. 统一 YES/NO token、price、size、aggressor_side。
5. price 必须在 [0,1]。
6. size 必须 > 0。
```

## 10.2 Taker fill 伪代码

```python
def fill_taker_orderfilled_only(order, trade_tape, config):
    arrival_ts = order.signal_ts + config.latency
    deadline = arrival_ts + config.execution_horizon
    remaining = order.size
    fills = []

    eligible = trade_tape.query(
        market_id=order.market_id,
        asset_id=order.asset_id,
        ts_start=arrival_ts,
        ts_end=deadline,
    )

    for trade in eligible:
        if order.side == "BUY":
            if trade.aggressor_side != "BUY":
                continue
            if trade.price > order.limit_price:
                continue

            capacity = trade.remaining_capacity(config.participation_rate)
            qty = min(remaining, capacity)
            if qty <= 0:
                continue

            exec_price = trade.price + config.taker_price_buffer(order, trade, qty)
            if exec_price > order.limit_price:
                continue

        else:
            if trade.aggressor_side != "SELL":
                continue
            if trade.price < order.limit_price:
                continue

            capacity = trade.remaining_capacity(config.participation_rate)
            qty = min(remaining, capacity)
            if qty <= 0:
                continue

            exec_price = trade.price - config.taker_price_buffer(order, trade, qty)
            if exec_price < order.limit_price:
                continue

        fills.append(Fill(order_id=order.id, price=exec_price, size=qty, source=trade.id))
        trade.consume_capacity(qty)
        remaining -= qty

        if remaining <= 0:
            break

    return fills, remaining
```

## 10.3 Maker fill 伪代码

```python
def admit_maker_orderfilled_only(order, config):
    if config.maker_mode == "strict_no_fill":
        order.state = "WORKING_BUT_NON_EXECUTABLE_IN_ORDERFILLED_ONLY"
        return order

    order.phantom_queue = estimate_phantom_queue(order, config)
    order.remaining = order.size
    return order
```

后续处理：

```python
def process_trade_for_maker(order, trade, config):
    if order.side == "BUY":
        if trade.aggressor_side != "SELL":
            return
        if trade.price > order.limit_price:
            return

    if order.side == "SELL":
        if trade.aggressor_side != "BUY":
            return
        if trade.price < order.limit_price:
            return

    flow = trade.remaining_capacity(config.maker_participation_rate)

    if order.phantom_queue > 0:
        consumed = min(order.phantom_queue, flow)
        order.phantom_queue -= consumed
        flow -= consumed

    if flow > 0:
        qty = min(order.remaining, flow)
        fill(order, price=order.limit_price, size=qty, source=trade)
```

---

# 11. 容量模型是准确性的核心

如果你用 `OrderFilled` 做回测，最重要的参数不是 slippage，而是：

```text
participation_rate
```

它代表：

```text
你的模拟订单最多能占历史真实成交量的多少比例？
```

传统 equity backtest 里，Zipline / QuantConnect 默认常用 2.5% 的 volume cap。([Zipline][4]) Polymarket 很多 market 比大股票薄得多，所以你不应该默认 10%、20%、50%。

建议：

```text
主结果:
    participation_rate = 1% 或 2.5%

保守:
    0.5% - 1%

现实:
    1% - 2.5%

乐观:
    5%

大于 10%:
    只能做 capacity stress，不做主结果
```

还要输出 capacity curve：

```text
participation_rate = 0.5%
participation_rate = 1%
participation_rate = 2.5%
participation_rate = 5%
participation_rate = 10%
```

如果策略只有在 10% 历史成交参与率下才赚钱，说明它不可扩容。

---

# 12. 这个模型适合哪些策略，不适合哪些策略

## 适合

```text
1. 小 size taker 策略
2. 信号后等待真实成交流出现再成交的策略
3. 以真实成交为触发条件的 momentum / mean-reversion
4. copy-trading / wallet-following 的保守容量估计
5. 事件后几分钟内参与成交流的策略
6. 只需要证明“历史市场确实有足够成交活动”的策略
```

## 不适合

```text
1. 做市 / spread capture
2. 纯 maker queue 策略
3. 依赖排队优先级的策略
4. 抢盘口顶部的策略
5. 需要精确 bid/ask spread 的策略
6. 大 size taker 策略
7. 需要证明“当时立刻可成交”的策略
```

对这些不适合的策略，如果没有 LOB，你只能给出：

```text
not supported under OrderFilled-only execution
```

这不是失败，而是诚实。

---

# 13. 报告应该怎么写，才算“漂亮”

你的回测报告不要只给 PnL。要给这些：

```text
Execution model:
    OrderFilled-only trade-tape participation

Data used:
    one-sided OrderFilled trade prints

LOB usage:
    none / stale / excluded

Orders:
    total signals
    orders attempted
    orders partially filled
    orders fully filled
    orders unfilled due to no same-side future trade
    orders unfilled due to limit price
    orders unfilled due to participation cap

Capacity:
    historical eligible volume
    simulated volume
    participation_rate
    simulated_volume / eligible_orderfilled_volume

Price:
    avg historical print price
    avg execution price
    price buffer paid
    fee

Sensitivity:
    conservative / realistic / optimistic
    participation-rate curve
    latency curve
    horizon curve
```

核心指标：

```text
fill_rate
partial_fill_rate
avg_fill_delay
avg_participation
avg_price_buffer
capacity_utilization
profit_after_fees
profit_after_settlement
PnL under conservative
PnL under realistic
PnL under optimistic
```

---

# 14. 如何测试这个 OrderFilled-only 模型是否有效

## 14.1 单元测试

必须测试：

```text
BUY taker 只能用 aggressor_side=BUY 的未来成交。
SELL taker 只能用 aggressor_side=SELL 的未来成交。
BUY limit 不能用 price > limit 的成交。
SELL limit 不能用 price < limit 的成交。
订单不能使用 signal_ts 之前的成交。
订单不能使用 arrival_ts 之前的成交。
一个历史 trade print 的 capacity 不能被多个模拟订单重复消耗。
总模拟成交量不能超过 participation_rate * eligible historical volume。
```

## 14.2 方向测试

构造：

```text
trade_1: BUY @ 0.52 size=1000
trade_2: SELL @ 0.51 size=1000
```

测试：

```text
BUY taker limit 0.53:
    可以用 trade_1
    不能用 trade_2

SELL taker limit 0.50:
    可以用 trade_2
    不能用 trade_1

BUY maker @ 0.51:
    可以被 trade_2 推进
    不能被 trade_1 推进

SELL maker @ 0.52:
    可以被 trade_1 推进
    不能被 trade_2 推进
```

这和 Nautilus 对 trade tick aggressor side 的说明一致：seller trade 为 buy limit 提供 fill evidence，buyer trade 为 sell limit 提供 fill evidence。([NautilusTrader][2])

## 14.3 不变量测试

```text
simulated_fill_size <= order_size
simulated_fill_size <= participation_rate * eligible_historical_volume
fill_ts >= arrival_ts
fill_price not better than historical trade price unless explicitly allowed
BUY fill_price <= limit_price
SELL fill_price >= limit_price
maker strict mode produces zero fills
no OrderFilled evidence -> no fill
same input -> same output
```

## 14.4 与真实钱包 replay 对账

选一些真实钱包：

```text
wallet A 在某时间附近买了 YES
```

用你的标准化 `OrderFilled` tape 重建它的 actual fill：

```text
actual price
actual size
actual direction
actual fee
```

然后用你的引擎输入一个“copy exact order”：

```text
arrival_ts = actual_trade_ts - small_delta
side = actual_side
limit_price = actual_price + allowed_buffer
size = actual_size * participation_rate
```

看引擎是否能在合理规则下复现：

```text
same direction
same price附近
size不超过capacity
```

这不能证明你能模拟所有未成交订单，但能证明你的成交证据、方向、价格、capacity ledger 没写错。

## 14.5 Forward paper / live 小单校准

HftBacktest 文档提醒，market replay 的关键假设是你的订单足够小、不影响市场，最终需要用 live market discrepancy 来校准。([HftBacktest][1]) 你应该用真实 paper/live 小单校准：

```text
participation_rate
latency
price_buffer
maker_phantom_queue
execution_horizon
```

指标：

```text
predicted fill probability vs actual
predicted fill size vs actual
predicted price vs actual
false positive fills
false negative fills
```

如果你的模型经常预测能成交但实盘没成交，说明 participation_rate 太高、latency 太低、price_buffer 太小、maker queue 太乐观。

## 14.6 统计稳健性测试

不要只优化一个参数组合。Bailey、Borwein、López de Prado 和 Zhu 的 backtest overfitting 论文指出，很多投资策略因为反复试验同一历史数据而产生虚假的优秀结果；他们提出用 combinatorially symmetric cross-validation 来估计 backtest overfitting probability。([IDEAS/RePEc][10]) Deflated Sharpe Ratio 也用于修正 multiple testing、非正态收益和选择偏差造成的 Sharpe 膨胀。([SSRN][11])

你的报告应该固定输出：

```text
out-of-sample markets
walk-forward windows
category split: crypto / sports / politics
regime split: high volume / low volume / near resolution / far from resolution
parameter sensitivity grid
```

不要只展示最优参数。

---

# 15. 一个完整的 OrderFilled-only 回测分层

我建议你的框架里有四个执行等级。

## Grade A：Observed Fill Replay

```text
策略本身就是分析真实历史成交、copy真实钱包、或复盘已发生交易。
```

这是最准确的，因为它不是强 counterfactual。

## Grade B：Trade-Tape Participation

```text
策略信号后，只能参与未来真实 OrderFilled 成交流的一小部分。
```

这是你在 LOB 缺失时最推荐的主模型。

## Grade C：Formula Slippage Baseline

```text
没有 LOB，也不用严格 OrderFilled capacity。
用 volume、volatility、price history 做经验滑点。
```

这个可以做参考，不作为主结果。类似 Zipline、QuantConnect、Backtrader、distank 这类 volume/slippage 路线。([Zipline][4])

## Grade D：Unsupported

```text
maker queue / 做市 / spread capture / 大单 depth execution
```

没有 LOB 时主结果不支持。

---

# 16. 最终建议你怎么落地

你现在的 NBA 样本 LOB coverage 很差，所以不要强行做 DEPTH。应该这样做：

```text
1. 把这些 market 标记为:
   l2_depth_usable = false
   execution_model = orderfilled_only_trade_tape

2. 标准化 OrderFilled:
   去重
   one-sided volume
   aggressor/passive side
   price/size
   tx group

3. 做 OrderFilled-only taker:
   future same-side trade prints
   participation cap
   latency
   limit check
   price buffer
   capacity ledger

4. maker 默认不进入主结果:
   如果要做，只能 phantom_queue sensitivity

5. 报告三套结果:
   strict
   conservative
   optimistic

6. 输出 capacity/sensitivity:
   0.5%, 1%, 2.5%, 5%, 10% participation

7. 明确声明:
   这不是 L2 depth execution。
   这是 fill-evidence constrained execution。
```

一句话总结：

**仅靠 `OrderFilled`，最准确的回测不是模拟盘口，而是模拟“你最多能参与历史真实成交流的多少”。**

这样做出来的回测会很漂亮，因为它：

```text
不虚构流动性；
不使用 stale LOB；
不把 touched price 当成交；
不双算 OrderFilled；
不允许超过历史成交容量；
每笔 fill 都能追溯到 tx_hash；
能给出容量、延迟、价格 buffer 的敏感性边界。
```

但你也要在报告标题里写清楚：

```text
OrderFilled-only results are execution-evidence constrained,
not full depth / queue-accurate simulations.
```

这才是缺失 LOB 数据时真正专业的设计。

[1]: https://hftbacktest.readthedocs.io/en/py-v2.1.0/order_fill.html "Order Fill — hftbacktest"
[2]: https://nautilustrader.io/docs/latest/concepts/backtesting/ "Backtesting | NautilusTrader"
[3]: https://arxiv.org/html/2602.00133v1 "PredictionMarketBench: A SWE-bench-Style Framework for Backtesting Trading Agents on Prediction Markets"
[4]: https://zipline.ml4trading.io/_modules/zipline/finance/slippage.html "zipline.finance.slippage — Zipline 3.0 docs"
[5]: https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/slippage/supported-models "Supported Models - QuantConnect.com"
[6]: https://www.backtrader.com/docu/filler/ "Broker - Volume Filling - Fillers - Backtrader"
[7]: https://github.com/distank/polymarket-backtest "GitHub - distank/polymarket-backtest: Open-source backtest simulator for Polymarket prediction markets · GitHub"
[8]: https://arxiv.org/html/2603.03136v2 "The Anatomy of a Blockchain Prediction Market: Polymarket in the 2024 U.S. Presidential Election"
[9]: https://www.paradigm.xyz/writing/polymarket-volume-is-being-double-counted "Polymarket Volume Is Being Double-Counted"
[10]: https://ideas.repec.org/a/rsk/journ0/2471206.html?utm_source=chatgpt.com "The probability of backtest overfitting - IDEAS/RePEc"
[11]: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551&utm_source=chatgpt.com "Correcting for Selection Bias, Backtest Overfitting and Non-Normality"
