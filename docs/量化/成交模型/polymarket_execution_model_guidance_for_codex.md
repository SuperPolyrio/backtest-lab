# Polymarket 回测执行模型指导文档：`OrderFilled + 部分 LOB` 成交系统

> 目标读者：Codex / 开发者  
> 目标：把现有 `OrderFilled` 链上成交数据 + 部分 L2 LOB 数据，落地成一个严肃但诚实的 Polymarket 回测执行模型。  
> 核心结论：不要把 `OrderFilled` 假装成 L3 MBO；要把它做成 `TradeTick / FillTick / execution evidence`，再和 L2 order book replay 组合成 `queue-aware execution model`。

---

## 0. 一句话原则

Polymarket 回测执行层不应该问：

```text
这个时间段价格有没有到过我的价格？
```

而应该问：

```text
我的订单到达市场时，盘口上有没有可见流动性？
如果我是 taker，我的 size 能吃几档、平均成交价是多少、是否 partial fill？
如果我是 maker，我前面估计有多少 queue？后续真实 OrderFilled 有没有把 queue 消耗到我？
LOB 变化中哪些是成交，哪些可能是撤单/更新？
```

所以执行层必须从 OHLCV/close-price 逻辑升级为：

```text
L2 BookState + OrderFilled-derived FillTick + latency + queue heuristic + residual liquidity accounting
```

---

## 1. 外部框架是怎么做的

### 1.1 `evan-kolberg/prediction-market-backtesting`：严肃路线，L2 book replay + TradeTick

这个仓库是最值得参考的路线。它的文档明确写了：

- 活跃 Polymarket 路径是 `L2 market-by-price book replay`。
- 策略消费 book state。
- `OrderBookDeltas` 更新 L2 book。
- `TradeTick` 只作为 execution evidence，用于 matching 和 queue-position update。
- bars 不驱动 execution。
- `queue_position=True` 时，resting limit order 被接受时，会 snapshot 同侧同价位的 displayed quantity，作为 estimated queue ahead；后续 trade ticks 在该价位出现时递减 queue ahead；只有超过 queue ahead 的成交量才可能 fill 你的模拟订单。
- 它自己也明确承认：这是 L2 MBP heuristic，不是 L3 MBO / true FIFO。

参考：
- https://github.com/evan-kolberg/prediction-market-backtesting/blob/v4.1-alpha/docs/execution-modeling.md
- https://github.com/evan-kolberg/prediction-market-backtesting/blob/v4.1-alpha/docs/data-vendors.md

开发启发：

```text
不要让 OHLCV 驱动成交。
不要让 last price 驱动成交。
BookDelta/Snapshot 负责维护“当时盘口”。
OrderFilled/TradeTick 负责证明“后来真实成交消耗”。
Maker fill 必须经过 queue gate。
```

---

### 1.2 `braedonsaunders/homerun`：工程化路线，L2 snapshots + latency + residual book + queue + impact

`homerun` 的 matching engine 描述得很清楚：输入是 L2 snapshots、venue model、latency model、策略订单意图；输出是 deterministic ledger，包括 fills、partial fills、rejections、cancel/replace transitions。

它的几个设计点很值得学：

1. **订单生命周期完整**

```text
pending_submit -> working -> partial -> filled / cancelled / rejected
```

2. **latency 是订单进入市场前的状态，不是回测装饰参数**

订单在 `submitted_at` 发出，但只有 `venue_received_at = submitted_at + submit_latency` 之后才能和 book 交互。

3. **残余盘口 residual book**

模拟 taker 吃掉了某一档可见流动性后，要在当前 snapshot interval 内记录已消耗数量，防止多个模拟订单重复吃同一份历史 depth。

4. **queue ahead tracking**

resting order 进入 book 时记录 `queue_ahead_shares`；后续外部 flow 消耗前方 queue；只有 queue 清零后才 eligible to fill。

5. **book decrease 不能全部当成交**

它把 disappeared depth 看成 `trades + cancels` 的混合，并用 `queue_progress_trade_fraction` 控制多少 disappearing depth 被当作推动 queue 的真实成交。这一点非常重要：如果把所有 book size 下降都当作成交，会严重高估 maker fill。

参考：
- https://github.com/braedonsaunders/homerun
- https://raw.githubusercontent.com/braedonsaunders/homerun/main/backend/services/backtest/matching_engine.py

开发启发：

```text
执行模型要是事件驱动、单线程、可复现。
每个 fill 都要可解释：来自哪张 snapshot、哪个 OrderFilled、哪个 queue 状态。
book 被模拟订单消耗后，必须维护 residual state，避免 double count。
```

---

### 1.3 `Oddpool/PredictionMarketBench`：清晰的 maker queue 模型

`PredictionMarketBench` 的论文/文档强调 event-driven deterministic replay，输入包括 orderbook updates、trade prints、settlement events，模拟器支持 maker/taker execution semantics 和 fee modeling。

它的 `maker_queue.py` 思路很适合你当前阶段：

```text
LevelQueue:
    env_ahead = 非 agent 的环境流动性
    agent_orders = 你的模拟订单 FIFO 队列

place_order:
    新 maker 单进入时，env_ahead = 当前 book 同价位 size

process_trade:
    历史 trade print 先消耗 env_ahead
    env_ahead 清零后，剩余 volume 才 fill agent orders

update_env_from_snapshot:
    trade_only 模式：只用 trade prints 推进 queue，保守
    reconciled 模式：用 snapshot 变化推断撤单，调整 env_ahead
```

参考：
- https://arxiv.org/html/2602.00133v1
- https://raw.githubusercontent.com/Oddpool/PredictionMarketBench/main/src/oddpool_bench/maker_queue.py

开发启发：

```text
你的 maker queue 先从 LevelQueue 模型做起。
第一版用 trade_only，第二版再做 reconciled。
```

---

### 1.4 `distank/polymarket-backtest`：轻量路线，price history + 公式滑点

这个仓库的执行模型是：

```text
spread = |P(yes) - (1 - P(no))|
slippage = min(1%, 1000 / daily_volume) 或页面展示中的 min(0.5%, 100 / daily_volume)
commission = 2%
fill_price(BUY)  = price + half_spread + slippage + commission adjustment
fill_price(SELL) = price - half_spread - slippage - commission adjustment
```

它的核心数据是 price history / volume，不是历史 L2 replay，也没有 queue。它适合产品 demo、快速研究、批量粗筛，不适合当你的 execution truth。

参考：
- https://github.com/distank/polymarket-backtest
- https://distank.github.io/polymarket-backtest/

开发启发：

```text
不要照抄这个做执行层。
它可以作为 baseline：用来证明你的 L2+OrderFilled 模型比公式滑点模型更保守、更接近真实成交约束。
```

---

## 2. Polymarket 交易机制对执行模型的约束

### 2.1 所有订单本质上都是 limit order

Polymarket 文档说明，所有订单都是 limit orders；所谓 market order 是设置一个可立即和 resting orders 撮合的 limit order。订单类型包括 GTC、GTD、FOK、FAK；post-only 订单如果会跨过 spread，则会被拒绝。

参考：
- https://docs.polymarket.com/concepts/order-lifecycle

执行模型含义：

```text
不要在代码里发明真正的“无限市价单”。
Market buy/sell = aggressive limit order。
FOK = 全部可成交才成交，否则 reject。
FAK/IOC = 能成交多少成交多少，剩余 cancel。
GTC/GTD = 先吃可成交部分，剩余可能 rest。
Post-only = 如果会 cross book，reject。
```

---

### 2.2 LOB 是 L2 MBP，不是 L3 MBO

Polymarket order book API / WebSocket 给的是 bids/asks，每档 price + size；market websocket 有 `book`、`price_change`、`last_trade_price`、`best_bid_ask` 等事件。`price_change` 中 `size = "0"` 表示该价位从 book 移除。

参考：
- https://docs.polymarket.com/api-reference/market-data/get-order-book
- https://docs.polymarket.com/market-data/websocket/market-channel

执行模型含义：

```text
你能看到某个价格总 size。
你看不到单个 order_id。
你看不到同价位内部真实 FIFO 顺序。
你看不到未成交订单的完整生命周期。
所以只能做 L2 queue heuristic，不能声称是 L3 FIFO。
```

---

### 2.3 `OrderFilled` 是成交证据，不是完整订单簿

Polymarket 文档说明，trade settled onchain 时，Exchange contract 会 emit `OrderFilled`，字段包括：

```text
orderHash
maker
taker
makerAssetId
takerAssetId
makerAmountFilled
takerAmountFilled
fee
```

其中：

```text
makerAssetId == 0 表示 maker 给出 pUSD、收到 outcome tokens，所以这个 maker order 是 BUY。
takerAssetId == 0 表示 maker 给出 outcome tokens、收到 pUSD，所以这个 maker order 是 SELL。
```

参考：
- https://docs.polymarket.com/trading/orders/overview

执行模型含义：

```text
OrderFilled 可以构造 FillTick / TradeTick。
OrderFilled 可以证明某个 price、side、size 真的成交过。
OrderFilled 不能告诉你没成交的订单在哪里，不能告诉你谁撤单了，不能告诉你你的模拟订单真实排在哪里。
```

---

## 3. 你的目标架构

建议把执行层命名为：

```text
L2OrderFilledExecutionModel
```

不要命名成：

```text
L3ExecutionModel
TrueFIFOExecutionModel
```

因为它不是 L3。

整体架构：

```text
Raw LOB snapshots / deltas
        ↓
BookEventNormalizer
        ↓
BookState / ResidualBook

Raw onchain OrderFilled
        ↓
OrderFilledNormalizer
        ↓
FillTick / TradeTick / ExecutionEvidence

Strategy signal
        ↓
StrategyOrderIntent
        ↓ latency
SimulatedOrder enters MatchingEngine
        ↓
Taker: walk visible book
Maker: queue_ahead + OrderFilled queue consumption + LOB cancellation reconciliation
        ↓
ExecutionFill ledger
        ↓
Portfolio accounting / metrics / audit logs
```

---

## 4. 数据模型设计

### 4.1 `BookSnapshot`

```python
@dataclass(frozen=True)
class BookSnapshot:
    ts: datetime
    market_id: str
    asset_id: str
    sequence: int | None
    source: str
    bids: list[BookLevel]  # sorted desc by price internally
    asks: list[BookLevel]  # sorted asc by price internally
    hash: str | None = None
    min_order_size: Decimal | None = None
    tick_size: Decimal | None = None
    is_full_depth: bool = False
    observed_depth_levels: int | None = None
```

注意：

```text
价格和数量内部用 Decimal 或整数 tick/cents，不要用 float 做核心撮合。
输入数据里的 bids/asks 排序不可信，进入 BookState 前必须排序。
```

---

### 4.2 `BookDelta`

```python
@dataclass(frozen=True)
class BookDelta:
    ts: datetime
    market_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]  # BUY means bid level, SELL means ask level
    price: Decimal
    new_size: Decimal
    sequence: int | None
    source: str
    hash: str | None = None
```

语义：

```text
new_size > 0: set price level size = new_size
new_size == 0: remove this price level
```

---

### 4.3 `FillTick`：由 `OrderFilled` 规范化而来

```python
@dataclass(frozen=True)
class FillTick:
    ts: datetime                 # 优先 match_time；没有就用 block_time
    block_number: int | None
    tx_hash: str
    log_index: int
    order_hash: str
    market_id: str
    asset_id: str                # outcome token id
    price: Decimal
    size: Decimal                # outcome token shares
    passive_side: Literal["BUY", "SELL"]
    aggressor_side: Literal["BUY", "SELL"]
    maker: str
    taker: str
    fee: Decimal
    source: Literal["orderfilled", "trade_api", "reconciled"]
```

`passive_side` 是被成交的 resting maker order 方向：

```text
makerAssetId == 0:
    passive_side = BUY
    aggressor_side = SELL
    size = takerAmountFilled      # maker received outcome tokens
    price = makerAmountFilled / takerAmountFilled

elif takerAssetId == 0:
    passive_side = SELL
    aggressor_side = BUY
    size = makerAmountFilled      # maker gave outcome tokens
    price = takerAmountFilled / makerAmountFilled
```

注意：

```text
上面的 size/price 需要按 token decimals 和 USDC decimals 规范化。
同一个 tx 内可能有多个 maker OrderFilled。
不要把每条 OrderFilled 都无脑当成独立 taker trade；对 maker queue 它们有用，但市场级 volume 可能需要按 taker order / tx 聚合。
```

---

### 4.4 `StrategyOrderIntent`

```python
@dataclass
class StrategyOrderIntent:
    client_order_id: str
    signal_ts: datetime
    market_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["LIMIT", "MARKETABLE_LIMIT"]
    limit_price: Decimal
    size: Decimal
    tif: Literal["GTC", "GTD", "FOK", "FAK", "IOC"]
    post_only: bool = False
    expires_at: datetime | None = None
```

执行时生成：

```python
venue_received_at = signal_ts + latency_model.sample_submit_latency(...)
```

---

### 4.5 `RestingOrder`

```python
@dataclass
class RestingOrder:
    order_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]
    price: Decimal
    original_size: Decimal
    remaining_size: Decimal
    accepted_ts: datetime
    queue_ahead: Decimal
    state: Literal["WORKING", "PARTIAL", "FILLED", "CANCELLED", "REJECTED"]
    fills: list[ExecutionFill]
```

---

### 4.6 `ExecutionFill`

```python
@dataclass(frozen=True)
class ExecutionFill:
    order_id: str
    ts: datetime
    asset_id: str
    side: Literal["BUY", "SELL"]
    liquidity_flag: Literal["MAKER", "TAKER"]
    price: Decimal
    size: Decimal
    fee: Decimal
    source_event_ids: list[str]
    reason: str
    book_ts: datetime | None
    queue_ahead_before: Decimal | None = None
    queue_ahead_after: Decimal | None = None
```

每一笔 fill 都必须可解释。

---

## 5. 事件时间线规则

所有数据进入一个统一事件流：

```python
events = merge_sort([
    BookSnapshotEvent,
    BookDeltaEvent,
    FillTickEvent,
    StrategyOrderIntentEvent,
    StrategyCancelIntentEvent,
    MarketLifecycleEvent,
])
```

推荐排序规则：

```text
1. event_ts 升序
2. 同 timestamp 内，先处理 book snapshot/delta
3. 再处理 historical FillTick / OrderFilled
4. 再处理 strategy signal
5. strategy order 必须经过 latency，变成 venue_received_at 后才进入 matching engine
6. 同一 tx 的 OrderFilled 按 log_index 升序
```

非常重要：

```text
如果 OrderFilled 只有 block_time，就只能在 block_time 之后使用它。
不能把链上结算事件提前到 CLOB 真实撮合时刻，除非你有可靠 match_time。
否则就是 look-ahead。
```

---

## 6. BookState 与数据缺口处理

### 6.1 BookState 更新规则

```python
class BookState:
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    last_snapshot_ts: datetime | None
    last_update_ts: datetime | None
    stale_after_ms: int
```

处理 snapshot：

```text
清空旧 book。
重建 bids/asks。
更新 last_snapshot_ts / last_update_ts。
```

处理 delta：

```text
如果没有初始化 snapshot：拒绝应用 delta，等待 snapshot。
如果中间有明显 gap：拒绝跨 gap 应用 delta，等待新 snapshot。
如果 new_size == 0：删除价位。
否则：set 价位 size = new_size。
```

### 6.2 LOB 不完整时的质量标记

每个 book state 都要有质量字段：

```python
@dataclass
class BookQuality:
    book_age_ms: int
    has_snapshot_anchor: bool
    gap_since_last_snapshot: bool
    depth_levels_observed: int
    is_full_depth: bool
    source: str
    confidence: Literal["HIGH", "MEDIUM", "LOW", "INVALID"]
```

使用规则：

```text
INVALID：不允许成交。
LOW：允许小 size，depth_haircut 很大。
MEDIUM：允许 walk 可见 book，但不能扫过最后一档。
HIGH：正常 walk visible book。
```

### 6.3 数据 gap 后必须 reset queue

如果 LOB 中断，例如缺小时、缺 snapshot、delta 不连续：

```text
重置 BookState。
取消或冻结所有依赖该 book 的 working maker orders。
不要继承旧 queue_ahead。
等待新 snapshot 后重新开始。
```

这是为了避免跨缺口虚构 FIFO 状态。

---

## 7. Taker 执行模型

### 7.1 Taker 定义

Taker 是主动吃掉已有 resting liquidity 的订单。

```text
BUY taker 吃 asks。
SELL taker 吃 bids。
```

Polymarket 里 market order 本质也是 marketable limit order，所以 taker 执行必须带 limit price。

---

### 7.2 Taker 执行步骤

输入：

```text
order.side
order.limit_price
order.size
order.tif
BookState at venue_received_at
```

步骤：

```text
1. 检查 book 是否有效、未 stale。
2. 根据 side 选择对手盘：BUY -> asks ascending；SELL -> bids descending。
3. 按价格逐档 walk book。
4. 不能超过 limit price：
   BUY 只能吃 price <= limit_price 的 asks。
   SELL 只能吃 price >= limit_price 的 bids。
5. 每档可成交量 = visible_size - residual_consumed。
6. 可以乘以 depth_haircut。
7. 生成一笔或多笔 ExecutionFill。
8. 更新 ResidualBook，防止后续模拟订单重复吃同一份 depth。
9. 根据 TIF 处理剩余：
   FOK：不能全填则整单 reject。
   FAK/IOC：成交可成交部分，剩余 cancel。
   GTC/GTD：成交可成交部分，剩余按 limit 规则 rest。
```

---

### 7.3 Taker 伪代码

```python
def execute_taker(order: StrategyOrderIntent, book: BookState) -> ExecutionResult:
    if not book.is_usable_at(order.venue_received_at):
        return reject(order, reason="book_stale_or_missing")

    remaining = order.size
    candidate_fills = []

    if order.side == "BUY":
        levels = book.remaining_asks_sorted_asc()
        executable = lambda p: p <= order.limit_price
    else:
        levels = book.remaining_bids_sorted_desc()
        executable = lambda p: p >= order.limit_price

    for price, visible_size in levels:
        if not executable(price):
            break

        capacity = visible_size * config.depth_haircut
        take = min(remaining, capacity)
        if take <= 0:
            continue

        adjusted_price = impact_model.apply_adverse_impact(
            side=order.side,
            price=price,
            take_size=take,
            visible_depth=book.visible_depth_on_consumed_side(order.side),
        )

        candidate_fills.append((adjusted_price, take, price))
        remaining -= take

        if remaining <= 0:
            break

    if order.tif == "FOK" and remaining > 0:
        return reject(order, reason="fok_insufficient_liquidity")

    # commit fills only after FOK check
    fills = []
    for adjusted_price, take, book_price in candidate_fills:
        fills.append(record_taker_fill(order, adjusted_price, take))
        book.residual_consume(side_taker=order.side, price=book_price, size=take)

    if remaining > 0 and order.tif in {"FAK", "IOC"}:
        return partial_then_cancel(order, fills, remaining)

    if remaining > 0 and order.tif in {"GTC", "GTD"}:
        resting = admit_maker_remainder(order, remaining, book)
        return partial_then_rest(order, fills, resting)

    return filled(order, fills)
```

---

### 7.4 Taker 的 `OrderFilled` 用途

Taker fill 不能完全由历史 `OrderFilled` 决定，因为这是 counterfactual：历史上别人没吃这档，不代表你不能吃。

`OrderFilled` 在 taker 模型中主要用于：

```text
1. 校验 LOB 是否可信。
2. 校准容量和 impact。
3. 统计你的模拟 taker volume 是否远超历史真实成交量。
4. 与 book change 对账，发现 LOB/OrderFilled 时间或方向异常。
```

不要写成：

```text
只有历史 OrderFilled 出现时，我的 taker 才能成交。
```

这是错误的，会低估 taker 可成交性。

---

## 8. Maker 执行模型

### 8.1 Maker 定义

Maker 是挂在 book 上等待别人来成交的订单。

```text
BUY maker resting at bid，等待 sell aggressor 打到这个 bid。
SELL maker resting at ask，等待 buy aggressor 打到这个 ask。
```

Maker 的难点不是价格，而是 queue：

```text
同价位前面已有多少环境订单？
后续真实成交是否先消耗完前方 queue？
LOB 下降是成交，还是撤单？
```

---

### 8.2 挂单准入规则

```python
def admit_maker(order, book):
    if order.post_only and would_cross(order, book):
        return reject("post_only_crosses_book")

    if order.tif in {"FOK", "FAK", "IOC"} and not marketable:
        return cancel_or_reject("non_resting_tif_not_filled")

    queue_ahead = estimate_queue_ahead(order, book)
    return RestingOrder(..., queue_ahead=queue_ahead)
```

`would_cross`：

```text
BUY limit_price >= best_ask -> would cross
SELL limit_price <= best_bid -> would cross
```

---

### 8.3 初始 queue_ahead 估计

默认保守：

```text
BUY maker @ P:
    queue_ahead = current bid size at P

SELL maker @ P:
    queue_ahead = current ask size at P
```

如果已有 agent 自己的订单在同价位更早挂着：

```text
queue_ahead += existing_agent_size_at_same_price_ahead
```

参数化：

```python
queue_ahead = same_price_visible_size * queue_ahead_fraction + agent_ahead
```

推荐默认：

```text
conservative: queue_ahead_fraction = 1.0
realistic:    queue_ahead_fraction = 0.65 ~ 1.0
optimistic:   queue_ahead_fraction = 0.3 ~ 0.65
```

第一版请用 `1.0`。

---

### 8.4 用 `OrderFilled` 推进 maker queue

对于 resting maker order：

```text
BUY @ P 只被 passive_side=BUY、price=P 的 FillTick 推进。
SELL @ P 只被 passive_side=SELL、price=P 的 FillTick 推进。
```

处理逻辑：

```python
def process_fill_tick_for_maker_queue(tick: FillTick):
    queue = queues[(tick.asset_id, tick.passive_side, tick.price)]
    remaining_flow = tick.size

    # 先消耗环境 queue
    if queue.env_ahead > 0:
        consumed = min(queue.env_ahead, remaining_flow)
        queue.env_ahead -= consumed
        remaining_flow -= consumed

    # env_ahead 清零后，才填 agent orders
    while remaining_flow > 0 and queue.agent_orders:
        order = queue.agent_orders[0]
        fill_size = min(order.remaining_size, remaining_flow)
        record_maker_fill(order, price=order.price, size=fill_size, source=tick)
        order.remaining_size -= fill_size
        remaining_flow -= fill_size

        if order.remaining_size <= 0:
            queue.agent_orders.pop(0)
```

重要：

```text
不能因为历史成交价触碰 P 就直接 fill 你的 maker 单。
必须先消耗 queue_ahead。
```

---

### 8.5 用 LOB price_change / snapshot change 推断撤单

假设：

```text
旧 bid 0.45 size = 1000
新 bid 0.45 size = 500
同 interval 内 OrderFilled passive_side=BUY price=0.45 size = 400
```

则：

```text
book decrease = 500
trade explained = 400
unexplained decrease = 100   # 可能是撤单/过期/修改/数据误差
```

不能简单把 500 全部当成交。推荐三种模式：

#### 模式 A：conservative / trade_only

```python
queue_ahead -= orderfilled_volume_only
cancel_ahead_fraction = 0.0
```

用途：上线前保守评估、容量边界、避免 maker fill 高估。

#### 模式 B：realistic / reconciled

```python
queue_ahead -= orderfilled_volume
queue_ahead -= inferred_cancel_volume * 0.3~0.5
```

用途：主结果。

#### 模式 C：optimistic

```python
queue_ahead -= orderfilled_volume
queue_ahead -= inferred_cancel_volume * 0.7~1.0
```

用途：上界，不要当真实主结果。

最终报告要输出三套结果：

```text
conservative_pnl
realistic_pnl
optimistic_pnl
```

如果策略只有 optimistic 赚钱，基本是 queue illusion。

---

### 8.6 Maker 例子

初始 book：

```text
10:00:00
bid 0.45 x 1000
ask 0.46 x 800
```

你的订单：

```text
10:00:01
BUY 300 @ 0.45 post_only
```

初始：

```text
queue_ahead = 1000
your_remaining = 300
```

后续事件：

```text
10:00:05 OrderFilled:
passive_side=BUY, price=0.45, size=400

10:00:05 LOB:
bid 0.45 size 1000 -> 500

10:00:08 OrderFilled:
passive_side=BUY, price=0.45, size=700
```

结果：

```text
conservative:
    10:00:05 queue_ahead = 1000 - 400 = 600
    10:00:08 700 flow 先消耗 600，剩余 100 fill 你
    your_fill = 100 / 300

realistic, cancel_ahead_fraction=0.5:
    unexplained decrease = 100
    10:00:05 queue_ahead = 1000 - 400 - 50 = 550
    10:00:08 剩余 150 fill 你
    your_fill = 150 / 300

optimistic, cancel_ahead_fraction=1.0:
    10:00:05 queue_ahead = 1000 - 400 - 100 = 500
    10:00:08 剩余 200 fill 你
    your_fill = 200 / 300
```

---

## 9. `OrderFilled` 规范化注意事项

### 9.1 价格和方向

对每条 raw `OrderFilled`：

```python
if makerAssetId == 0:
    # maker gave USDC/pUSD, received outcome token
    passive_side = "BUY"
    aggressor_side = "SELL"
    asset_id = takerAssetId
    size = normalize_token_amount(takerAmountFilled)
    price = normalize_usdc_amount(makerAmountFilled) / size

elif takerAssetId == 0:
    # maker gave outcome token, received USDC/pUSD
    passive_side = "SELL"
    aggressor_side = "BUY"
    asset_id = makerAssetId
    size = normalize_token_amount(makerAmountFilled)
    price = normalize_usdc_amount(takerAmountFilled) / size

else:
    # 复杂路径或组合路径，先标记 unsupported / needs_reconciliation
    skip_or_route_to_special_handler()
```

### 9.2 去重和聚合

必须保留 maker-level tick：

```text
tx_hash + log_index + orderHash
```

对 maker queue 来说，maker-level `OrderFilled` 是有用的。  
但对市场级成交量和 OHLCV，需要小心 many-to-one 情况，避免把一次 taker sweep 的多个 maker fills 重复解释成多次独立市场冲击。

建议同时产出两张表：

```text
fill_ticks_maker_level
trade_ticks_aggregated_by_tx_or_ordersmatched
```

如果暂时没有 `OrdersMatched`，先只用 maker-level 表驱动 maker queue，并在 market volume 报告里标注可能重复。

### 9.3 时间戳

优先级：

```text
1. CLOB trade API match_time
2. WebSocket last_trade_price timestamp
3. onchain block_time
```

如果只有 block_time：

```text
OrderFilled 只能在 block_time 生效。
不要提前用来填 maker。
```

---

## 10. 费用、rebate、latency、impact

### 10.1 费用

第一版建议：

```text
fee_model 必须统一在 ExecutionFill 层处理。
不要在策略层、portfolio 层、execution 层重复扣费。
```

最低字段：

```python
fee = fee_model.calculate(
    price=fill.price,
    size=fill.size,
    liquidity_flag="MAKER" or "TAKER",
    market_metadata=...
)
```

### 10.2 Latency

至少支持：

```python
submit_latency_ms
cancel_latency_ms
replace_latency_ms
```

默认配置：

```text
zero_latency: 用于理论上界，不作为主结果
static_latency: 例如 75ms / 250ms
sampled_latency: p50/p75/p95 分布
```

Polymarket 部分市场有 taker delay；有 market metadata 时，要按 market 配置处理。没有 metadata 时，不要硬编码成所有市场都 250ms。

### 10.3 Impact / capacity

L2 walk book 已经能计算 visible depth impact，但仍然可能乐观：真实市场看到大 taker flow 后可能撤单或变价。

建议支持可关的 adverse impact：

```text
impact_bps = strength_bps * sqrt(order_size / visible_depth)
BUY: adjusted_price = price * (1 + impact_bps)
SELL: adjusted_price = price * (1 - impact_bps)
```

默认可以关掉，但报告里要输出：

```text
simulated_taker_volume / historical_orderfilled_volume
order_size / visible_depth
```

如果策略容量明显超过历史成交量，回测结果不可信。

---

## 11. Codex 开发任务拆分

### Task 1：数据类型与 Decimal 工具

新增：

```text
src/backtest/types.py
src/backtest/decimal_utils.py
```

完成：

```text
BookLevel
BookSnapshot
BookDelta
FillTick
StrategyOrderIntent
RestingOrder
ExecutionFill
ExecutionConfig
```

验收：

```text
所有价格/size/notional 用 Decimal 或整数 tick。
所有 dataclass 都有 repr-safe 字段。
所有事件都有 stable event_id。
```

---

### Task 2：`OrderFilledNormalizer`

新增：

```text
src/backtest/orderfilled_normalizer.py
```

完成：

```text
raw OrderFilled -> FillTick
side 映射
price 计算
token decimals / USDC decimals normalize
tx_hash + log_index 去重
异常路径 quarantine
```

验收：

```text
makerAssetId==0 得到 passive_side=BUY。
takerAssetId==0 得到 passive_side=SELL。
price 在 [0,1]。
size > 0。
同一 raw event 不会重复导入。
```

---

### Task 3：`BookState` 与 `ResidualBook`

新增：

```text
src/backtest/book_state.py
```

完成：

```text
apply_snapshot
apply_delta
best_bid/best_ask/mid/spread
remaining_bids/remaining_asks
residual_consume
book_quality
stale/gap detection
```

验收：

```text
snapshot 重建 book。
delta size=0 删除价位。
bids 内部降序，asks 内部升序。
同一 snapshot interval 内不会 double count 被模拟订单吃掉的 depth。
```

---

### Task 4：Taker matching

新增：

```text
src/backtest/execution_taker.py
```

完成：

```text
walk book
FOK/FAK/IOC/GTC 处理
post-only cross reject
partial fill
residual book consumption
fee
impact optional
```

验收：

```text
BUY 只吃 asks。
SELL 只吃 bids。
BUY 不吃超过 limit_price 的 ask。
SELL 不吃低于 limit_price 的 bid。
FOK 不足全量时没有任何 partial fill。
```

---

### Task 5：Maker queue

新增：

```text
src/backtest/maker_queue.py
```

完成：

```text
LevelQueue(asset_id, side, price)
env_ahead
agent_orders FIFO
place_order
cancel_order
process_fill_tick
update_env_from_lob_change(mode=trade_only/reconciled)
```

验收：

```text
新 maker 单 queue_ahead = 同侧同价位 visible size * fraction + agent_ahead。
FillTick 先消耗 env_ahead，再 fill agent。
trade_only 不用 snapshot decrease 推进 queue。
reconciled 用 cancel_ahead_fraction 调整 queue。
queue_ahead 永不小于 0。
```

---

### Task 6：MatchingEngine 总线

新增：

```text
src/backtest/execution_engine.py
```

完成：

```text
merge event timeline
latency queue
submit/cancel lifecycle
BookState update
FillTick queue update
Taker/Maker routing
ExecutionFill ledger
```

验收：

```text
同样输入重复运行，fills 完全一致。
event_ts 非单调时抛错或排序校正并记录。
没有 book 时不成交。
没有 FillTick 时 maker 不凭空成交。
```

---

### Task 7：审计日志与解释器

新增：

```text
src/backtest/execution_audit.py
```

每笔订单输出：

```text
order_id
submitted_at
venue_received_at
state
fills
reject_reason
book_snapshot_id
queue_ahead_at_admit
queue_ahead_before_each_tick
queue_ahead_after_each_tick
source FillTick event ids
book_quality
mode: conservative/realistic/optimistic
```

验收：

```text
任何 fill 都能追溯到 book liquidity 或 OrderFilled evidence。
任何 reject/cancel 都有 reason。
```

---

## 12. 默认配置建议

### v1：保守主结果

```python
ExecutionConfig(
    mode="conservative",
    queue_ahead_fraction=1.0,
    cancel_ahead_fraction=0.0,
    depth_haircut=0.7,
    require_fresh_book=True,
    book_ttl_ms=5_000,
    allow_cross_gap_execution=False,
    use_orderfilled_for_maker_queue=True,
    use_lob_decrease_for_queue=False,
    latency_model=StaticLatencyModel(submit_ms=100, cancel_ms=50),
    impact_model=None,
)
```

### v2：现实主结果

```python
ExecutionConfig(
    mode="realistic",
    queue_ahead_fraction=0.8,
    cancel_ahead_fraction=0.5,
    depth_haircut=0.85,
    require_fresh_book=True,
    book_ttl_ms=5_000,
    allow_cross_gap_execution=False,
    use_orderfilled_for_maker_queue=True,
    use_lob_decrease_for_queue=True,
    latency_model=SampledLatencyModel(...),
    impact_model=SquareRootImpactModel(strength_bps=10),
)
```

### v3：上界结果

```python
ExecutionConfig(
    mode="optimistic",
    queue_ahead_fraction=0.5,
    cancel_ahead_fraction=1.0,
    depth_haircut=1.0,
    require_fresh_book=True,
    book_ttl_ms=10_000,
    allow_cross_gap_execution=False,
    use_orderfilled_for_maker_queue=True,
    use_lob_decrease_for_queue=True,
    latency_model=StaticLatencyModel(submit_ms=0, cancel_ms=0),
    impact_model=None,
)
```

报告中必须同时展示三者。不要只展示 optimistic。

---

## 13. 如何测试成交模型是否有效

### 13.1 单元测试：Taker

#### 测试 1：单档完全成交

Book：

```text
asks: 0.46 x 100
```

Order：

```text
BUY 50 limit 0.46 FAK
```

期望：

```text
fill 50 @ 0.46
remaining 0
```

#### 测试 2：多档 walk book

Book：

```text
asks:
0.46 x 100
0.47 x 200
0.50 x 1000
```

Order：

```text
BUY 250 limit 0.47 FAK
```

期望：

```text
100 @ 0.46
150 @ 0.47
avg = (100*0.46 + 150*0.47) / 250
```

#### 测试 3：limit 不允许吃更差价格

Order：

```text
BUY 400 limit 0.47 FAK
```

期望：

```text
只成交 300，不吃 0.50。
```

#### 测试 4：FOK 不足全量 reject

Order：

```text
BUY 400 limit 0.47 FOK
```

期望：

```text
0 fill
state = REJECTED
reason = fok_insufficient_liquidity
```

#### 测试 5：residual book 防 double count

Book：

```text
asks: 0.46 x 100
```

两个订单同 snapshot interval：

```text
BUY 80 limit 0.46
BUY 80 limit 0.46
```

期望：

```text
第一单 fill 80
第二单最多 fill 20
不能两单都 fill 80
```

---

### 13.2 单元测试：Maker queue

#### 测试 1：queue ahead 未清零不成交

初始：

```text
bid 0.45 x 1000
你的 BUY 100 @ 0.45
queue_ahead = 1000
```

FillTick：

```text
passive_side=BUY price=0.45 size=500
```

期望：

```text
queue_ahead = 500
你的 fill = 0
```

#### 测试 2：queue 清零后 partial fill

接着 FillTick：

```text
passive_side=BUY price=0.45 size=600
```

期望：

```text
先消耗 queue_ahead 500
剩余 100 fill 你的订单
```

#### 测试 3：错误方向不推进 queue

你的订单：

```text
BUY @ 0.45
```

FillTick：

```text
passive_side=SELL price=0.45 size=1000
```

期望：

```text
不推进这个 BUY queue。
```

#### 测试 4：错误价格不推进 queue

你的订单：

```text
BUY @ 0.45
```

FillTick：

```text
passive_side=BUY price=0.44 size=1000
```

期望：

```text
不推进 0.45 queue。
```

#### 测试 5：trade_only vs reconciled

初始：

```text
queue_ahead = 1000
book size 1000 -> 500
OrderFilled volume = 400
```

期望：

```text
trade_only: queue_ahead = 600
reconciled cancel_ahead_fraction=0.5: queue_ahead = 550
optimistic cancel_ahead_fraction=1.0: queue_ahead = 500
```

#### 测试 6：agent 自己同价位 FIFO

初始 book：

```text
bid 0.45 x 100
```

你的订单：

```text
A BUY 50 @ 0.45
B BUY 50 @ 0.45
```

FillTick：

```text
passive_side=BUY price=0.45 size=175
```

期望：

```text
先消耗 env_ahead 100
A fill 50
B fill 25
```

---

### 13.3 单元测试：OrderFilled normalizer

#### 测试 1：maker buy

Raw：

```text
makerAssetId = 0
makerAmountFilled = 45 USDC
takerAssetId = YES_TOKEN
takerAmountFilled = 100 YES
```

期望：

```text
asset_id = YES_TOKEN
passive_side = BUY
aggressor_side = SELL
size = 100
price = 0.45
```

#### 测试 2：maker sell

Raw：

```text
makerAssetId = YES_TOKEN
makerAmountFilled = 100 YES
takerAssetId = 0
takerAmountFilled = 45 USDC
```

期望：

```text
asset_id = YES_TOKEN
passive_side = SELL
aggressor_side = BUY
size = 100
price = 0.45
```

#### 测试 3：异常价格

Raw 计算出：

```text
price < 0 or price > 1
```

期望：

```text
quarantine，不进入执行流。
```

---

### 13.4 不变量测试 / property tests

必须长期成立：

```text
fill.size > 0
order.filled_size <= order.original_size
queue_ahead >= 0
taker BUY fill.price <= limit_price，未加 adverse impact 时成立；加 impact 后 adjusted price 也必须不超过 max allowed policy
taker SELL fill.price >= limit_price，未加 adverse impact 时成立
maker fill.price == order.limit_price
post_only crossing order never becomes taker
FOK 要么 full fill，要么 0 fill
没有 book evidence，不允许 taker fill
没有 OrderFilled/queue-cross evidence，不允许 maker fill
同样输入重复运行，输出完全一致
任何 fill 都有 source_event_ids 或 book_snapshot_id
```

---

### 13.5 回放一致性测试

用真实历史市场做 replay，不跑策略，只做数据对账。

指标：

```text
OrderFilled price 是否大多落在当时或附近 LOB bid/ask 可解释范围内
OrderFilled passive_side=BUY 时，对应 bid side 是否有相关 price level
OrderFilled passive_side=SELL 时，对应 ask side 是否有相关 price level
LOB size decrease 与 OrderFilled volume 的解释比例
price_change size=0 是否正确删除价位
snapshot gap 后是否 reset book
```

输出：

```text
coverage_ratio
book_age_distribution
orderfilled_matched_to_book_ratio
unexplained_book_decrease_ratio
unexplained_orderfilled_ratio
```

如果大量 OrderFilled 无法被附近 book 解释，说明：

```text
LOB 时间戳不对
OrderFilled block_time 太晚
asset_id 映射错
YES/NO canonical price 映射错
数据缺口太大
```

---

### 13.6 Shadow / live 校准测试

如果能做 paper/live 小单，最有价值。

流程：

```text
1. 实盘发出小 size maker/taker 订单。
2. 记录 submitted_at、venue_received_at、order ack、live fill、cancel ack。
3. 用同一时段的 LOB + OrderFilled 回放。
4. 比较模拟 fill 和真实 fill。
```

指标：

```text
Taker:
    predicted fill price vs actual fill price
    predicted fill size vs actual fill size
    reject/partial/full 分类准确率

Maker:
    predicted fill probability vs actual filled/unfilled
    predicted time-to-fill vs actual time-to-fill
    false positive fill rate
    false negative fill rate
    Brier score / calibration curve
```

用这些指标校准：

```text
queue_ahead_fraction
cancel_ahead_fraction
depth_haircut
book_ttl_ms
latency_model
impact_bps
```

---

### 13.7 与 OHLCV / 公式滑点模型对比

跑同一策略，输出三组结果：

```text
A. OHLCV / close-price execution
B. price_history + formula slippage baseline
C. L2 + OrderFilled execution
```

预期：

```text
C 的成交率通常更低。
C 的 maker fill 更少但更可信。
C 的大单平均成交价更差。
C 的 PnL 更保守。
如果 C 比 A/B 好很多，要检查是否有 look-ahead 或 queue 过乐观。
```

---

### 13.8 多 regime 压力测试

至少覆盖：

```text
高流动性 crypto 5min 市场
低流动性事件市场
spread 很宽的市场
临近 resolution 的市场
价格接近 0.01 / 0.99 的市场
tick size change 场景
LOB 缺口场景
OrderFilled 很密集的 sweep 场景
只有稀疏 LOB 的场景
```

每个 regime 都报告：

```text
fill rate
partial fill rate
average slippage
queue wait time
unfilled/cancelled ratio
PnL conservative/realistic/optimistic spread
```

---

## 14. 开发反模式清单

不要做：

```text
价格 touched 就 fill maker。
close price 到了就 fill。
用未来 snapshot 判断当前是否成交。
把 block OHLCV 当 execution truth。
把每个 OrderFilled 当 L3 FIFO。
把 book size decrease 全部当成交。
忽略撤单/取消 latency。
多个模拟订单重复吃同一份 book depth。
不处理 book stale/gap。
不区分 YES/NO token 和 BUY/SELL 方向。
用 float 做核心价格/size。
FOK 不足时还留下 partial fill。
post-only cross 时偷偷按 taker fill。
只输出一个 optimistic PnL。
```

---

## 15. 最小可行版本优先级

### Phase 1：保守可用版

实现：

```text
BookState from snapshots/deltas
OrderFilled -> FillTick
Taker walk visible book
Maker queue trade_only
ResidualBook
FOK/FAK/GTC/post-only
ExecutionFill ledger
unit tests
```

不做：

```text
复杂 impact
survival fill probability
maker rebate 精细化
cross-market neg risk
L3 FIFO
```

### Phase 2：reconciled 版

增加：

```text
LOB size decrease 对账
cancel_ahead_fraction
三模式结果
数据质量指标
regime test
```

### Phase 3：校准版

增加：

```text
paper/live shadow fills
latency distribution
fill probability calibration
capacity curve
strategy-level execution attribution
```

---

## 16. 最终验收标准

这个执行模型写对的标志不是 PnL 更漂亮，而是：

```text
1. 任何成交都能解释来源。
2. Taker 成交来自订单到达时可见 LOB depth。
3. Maker 成交来自 queue_ahead 被真实 OrderFilled 或保守的 LOB reconciliation 消耗。
4. 数据缺口不会被偷偷跨越。
5. 同一份 depth 不会被重复消费。
6. conservative <= realistic <= optimistic 的 fill 数量关系大体成立。
7. 策略在 optimistic 下赚钱但 conservative 下亏损时，报告能明确指出 execution sensitivity。
8. 回放真实数据时，OrderFilled 与 LOB 的方向、价格、时间大体可对账。
9. 单元测试覆盖所有 TIF、maker/taker、partial fill、queue、gap、stale book、方向映射。
10. 文档明确声明：这是 L2 MBP + OrderFilled heuristic，不是 L3 FIFO。
```

---

## 17. 给 Codex 的实现提示

优先写测试，再写引擎。

建议顺序：

```text
1. 写 `OrderFilledNormalizer` tests。
2. 写 `BookState` tests。
3. 写 `TakerExecution` tests。
4. 写 `MakerQueue` tests。
5. 写 `ExecutionEngine` integration tests。
6. 再接真实数据。
```

每个模块都要做到：

```text
deterministic
pure where possible
audit-friendly
no look-ahead
no hidden optimistic default
```

默认主结果用 conservative 或 realistic，不要用 optimistic 当展示结果。
