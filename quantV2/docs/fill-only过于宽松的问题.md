你这个现象不是“代码规则错了”，而是 **fill-only 模型的可成交条件太宽了**。

从你上传的 LOB holdout 看，order-level side-by-side 一共 9 个样本，只有 4 个是 `both_filled_same`，另外 5 个是 `fill_only_only`：也就是 fill-only 认为成交，但 depth+fill 认为拒绝。BUY-only 报告里也类似：9 个样本里 4 个 `both_filled_same`，4 个 `fill_only_only`，还有 1 个 `depth_only`。这说明你现在的 fill-only 模型在这个 holdout 上**假阳性偏高**，尤其危险的是 `fill_only_only`，因为它会让回测成交率和 PnL 偏乐观。 

核心原因是：

```text
fill-only 当前逻辑：
    只要未来有同方向、价格满足 limit 的真实成交 print，
    就认为我的订单可以参与其中。

LOB depth 逻辑：
    我的订单到达那一刻，盘口对手侧必须真的有可见 depth，
    并且价格、数量、TIF、book freshness 都满足条件。
```

这两个条件不是等价的。

---

## 1. 先把错误类型分清楚

你的报告里有两类不一致。

### A. `fill_only_only`：fill-only 过度乐观

例子：

```text
fifwc-nor-sen-2026-06-22-sen
order: BUY 40 @ 0.710001
fill-only: FILLED 40 @ 0.710000
depth+fill: REJECTED
```

还有：

```text
fifwc-fra-irq-2026-06-22-irq
order: BUY 31.25 @ 0.968001
fill-only: FILLED 31.25 @ 0.968000
depth+fill: REJECTED
```

这类是最需要修的。因为 fill-only 用了真实历史成交作为“我也能成交”的证据，但 LOB 告诉你：在订单到达那一刻，盘口并不支持这个成交。

### B. `depth_only`：fill-only 过度保守

BUY-only 报告里有一个：

```text
fifwc-nor-sen-2026-06-22-nor
order: BUY 10.780242 @ 0.410000
fill-only: NO_FILL
depth+fill: FILLED 10.780242 @ 0.410000
```

这说明 LOB 当时有可见 ask，可以被你的 taker 订单吃掉，但未来 trade tape 没有给 fill-only 足够的同方向成交证据。

这个问题在纯 fill-only 模型里**无法完全修复**，因为你没有 LOB，就不知道当时有 resting liquidity。你只能通过“交易 tape 推断报价”做弱修复，但那会变成 `fill-only + inferred quote`，不再是严格 fill-evidence 模型。

所以优先级应该是：

```text
先压低 fill_only_only 假阳性；
接受一部分 depth_only 假阴性。
```

fill-only 的定位本来就应该是保守的成交证据模型，而不是完整 L2/L3 盘口模型。

---

## 2. 你现在的 fill-only 条件太弱

现在大概率是这样的条件：

```text
BUY:
    future trade aggressor_side = BUY
    trade.price <= order.limit_price
    trade.ts >= order.arrival_ts
    trade.ts <= order.deadline_ts
    capacity 有剩余
    => fill

SELL:
    future trade aggressor_side = SELL
    trade.price >= order.limit_price
    trade.ts >= order.arrival_ts
    trade.ts <= order.deadline_ts
    capacity 有剩余
    => fill
```

这套规则通过了基本验证，但它只能证明：

```text
历史上未来出现过同方向成交。
```

它不能证明：

```text
我的订单到达时盘口上有可立即吃的 depth。
```

所以要把规则改成：

```text
future same-side trade print 是必要证据之一，
但不再是充分条件。
```

---

## 3. 第一处要改：禁止“source trade 自我填充”

你的 holdout 样本看起来是从真实 source trade 构造出来的，例如 source 是：

```text
ada9135bd794:1100
8c535fbac4e1:1156
8cdef9677c23:784
```

然后 order 的 limit 通常是：

```text
trade_price + 0.000001
```

fill-only 又用这个 source trade 把订单填掉。

这在 **plumbing validation** 里可以接受，因为你只是测试“能不能复现一笔真实成交”。

但在 **counterfactual 回测 / LOB holdout** 里要非常小心。如果订单意图是从这笔成交本身生成的，那么用同一笔成交作为 fill evidence 就有未来函数/自证风险。

建议新增规则：

```text
如果 order.signal_source_trade_id 不为空，
则 fill-only matching 必须排除这个 source_trade_id。
```

配置：

```python
exclude_signal_source_trade = True
```

伪代码：

```python
if trade.trade_id == order.signal_source_trade_id:
    continue
```

同时，arrival 也要严格处理：

```text
如果策略信号来自 trade T，
那么 order.arrival_ts 必须 > T.ts + latency。
不能用 T 自己成交。
```

这样很多 `fill_only_only` 会自然消失。

---

## 4. 第二处要改：加 pre-arrival trade-quote gate

没有 LOB 时，你可以用历史成交 tape 做一个**弱报价代理**。

逻辑是：

```text
最近的 BUY aggressor trade price ≈ 最近有人愿意付出的 ask-side 成交价
最近的 SELL aggressor trade price ≈ 最近有人愿意接受的 bid-side 成交价
```

所以：

```text
BUY 订单：
    不仅要求未来有 BUY trade，
    还要求 arrival 前有 fresh BUY trade quote proxy，
    且 pseudo_ask <= limit_price。

SELL 订单：
    不仅要求未来有 SELL trade，
    还要求 arrival 前有 fresh SELL trade quote proxy，
    且 pseudo_bid >= limit_price。
```

示例：

```python
pseudo_ask = last_pre_arrival_trade_price(
    aggressor_side="BUY",
    asset_id=order.asset_id,
    lookback=quote_proxy_ttl,
)

if order.side == "BUY":
    if pseudo_ask is None:
        reject("missing_pre_arrival_trade_quote_proxy")
    if pseudo_ask > order.limit_price:
        reject("pre_arrival_trade_quote_above_limit")
```

配置建议：

```python
quote_proxy_ttl = 5 minutes / 30 minutes / 1 hour
require_pre_arrival_quote_proxy = True
```

这不是 LOB，但它可以避免这种情况：

```text
未来孤立出现了一笔 BUY @ 0.71，
fill-only 就认为我也能 BUY @ 0.71，
但 arrival 前没有任何证据显示这个价格附近持续可成交。
```

---

## 5. 第三处要改：加 trade-density gate，拒绝孤立成交

很多 fill-only 假阳性来自“单笔未来成交”。
你应该要求一个订单的成交不是只靠一条孤立 print。

新增几个 gate：

```text
trailing_same_side_trade_count >= K
trailing_same_side_volume >= V
future_eligible_trade_count >= K2
future_eligible_volume >= V2
```

保守配置可以是：

```python
min_trailing_same_side_trade_count = 2
min_trailing_same_side_volume = max(order.size * 2, min_volume_floor)
min_future_eligible_trade_count = 2
```

或者更简单：

```python
if only_one_future_trade_and_no_recent_trade_context:
    reject("isolated_trade_print_not_enough_evidence")
```

这会降低 `fill_only_only`，但会增加 `depth_only`。这对 fill-only 主模型是可以接受的，因为主目标应该是保守、可审计，而不是尽可能接近 LOB 的所有 fill。

---

## 6. 第四处要改：加 worse-price buffer，不能按裸 trade price 填

你现在 holdout 订单很多是：

```text
BUY @ trade_price + 0.000001
```

而 fill-only 直接：

```text
FILLED @ trade_price
```

这太宽松了。报告里 `price_tolerance = 0.000001`，很多 mismatch 的 price delta 是 0，这说明 fill-only 没有给自己加足够的成交不确定性惩罚。

fill-only 应该永远使用 worse-price rule：

```text
BUY:
    exec_price = historical_trade_price + price_buffer

SELL:
    exec_price = historical_trade_price - price_buffer
```

然后再检查 limit：

```python
if order.side == "BUY":
    exec_price = trade.price + price_buffer
    if exec_price > order.limit_price:
        reject("price_buffer_exceeds_limit")

if order.side == "SELL":
    exec_price = trade.price - price_buffer
    if exec_price < order.limit_price:
        reject("price_buffer_exceeds_limit")
```

`price_buffer` 不要用 `0.000001`，应该用：

```text
至少 1 tick
或者 0.005
或者基于 price bucket / market activity 的 calibrated buffer
```

你的原始 fill-only 验证报告里已经有 `avg_price_buffer = 0.005` 的 conservative 模式；这个方向是对的。

所以 holdout comparison 也应该跑三套：

```text
plumbing mode:
    price_buffer = 0
    只用于复现真实成交，不用于回测主结果

strict mode:
    price_buffer = 1 tick 或 0.005/0.01
    participation_rate 低
    source trade exclusion 开启

conservative mode:
    price_buffer >= 1 tick
    pre-arrival quote gate 开启
```

如果用 `price_buffer = 0.005`，很多 `BUY @ trade_price + 0.000001` 的 fill-only-only 会被拒绝，从而更接近 depth reject。

---

## 7. 第五处要改：capacity 不能只看 future trade size

现在 fill-only 的容量大概是：

```text
capacity = future_trade.size * participation_rate
```

这仍然可能太乐观。你可以改成：

```text
capacity = min(
    future_eligible_volume * future_participation_rate,
    trailing_same_side_volume * trailing_participation_rate,
    absolute_order_cap,
    per_market_window_cap
)
```

也就是说，未来成交量不是唯一容量来源，还要受历史活跃度约束。

示例：

```python
future_capacity = future_eligible_volume * config.future_participation_rate
trailing_capacity = trailing_same_side_volume * config.trailing_participation_rate
absolute_capacity = config.max_fill_size_per_order

available_capacity = min(future_capacity, trailing_capacity, absolute_capacity)
```

如果 arrival 前没有同方向成交上下文：

```text
trailing_capacity = 0
=> 不成交
```

这个规则很适合降低孤立 `fill_only_only`。

---

## 8. 第六处要改：TIF-sensitive horizon

fill-only 里最容易虚高的参数是 `execution_horizon`。
如果你让一个“立即成交型”订单用 30s、5m、1h 去等未来成交流，那就会把后面不相关的成交拿来补自己。

所以 horizon 必须按 TIF 分开：

```text
FAK / IOC:
    horizon = 0s ~ 1s 或 1 block
    只能非常近的成交 print 作为证据

FOK:
    同一个 very short window 内必须全量满足，否则 reject

GTC / GTD:
    可以 longer horizon，但必须报告 fill delay
    并且 capacity / quote gate 更严格

paper taker / marketable limit:
    不应该用很长 horizon
```

配置：

```python
horizon_by_tif = {
    "FOK": "0s/1block",
    "FAK": "0s/1block",
    "IOC": "0s/1block",
    "GTC": "30s/5m depending strategy",
    "GTD": "until expiry but strict audit"
}
```

如果你的 holdout order 是模拟 taker immediate execution，那就不应该用很长 future tape horizon。

---

## 9. 第七处要改：用 LOB holdout 离线校准 fill-only 参数

你说“不添加 LOB”，我理解为**运行时不使用 LOB**。
但你已经有 LOB holdout，它可以作为离线标签来校准 fill-only 参数。

你可以建立一个：

```text
FillOnlyLobValidityModel
```

输入只用 fill tape 可获得的特征：

```text
side
limit_price
price_bucket
time_to_resolution
last_same_side_trade_age
last_opposite_side_trade_age
trailing_same_side_volume_5m
trailing_same_side_count_5m
trailing_opposite_volume_5m
future_eligible_trade_count
future_eligible_volume
trade_density
price_buffer_ticks
participation_rate
horizon
```

标签来自 LOB holdout：

```text
label = 1 if depth+fill would fill
label = 0 if depth+fill would reject
```

第一版不需要复杂 ML，可以直接做规则网格搜索：

```text
min_quote_freshness
min_trailing_volume
min_future_trade_count
price_buffer
participation_rate
horizon
```

优化目标不要是最大成交率，而是：

```text
尽量压低 fill_only_only
同时接受一定 depth_only
```

目标函数可以是：

```text
loss =
    5.0 * false_positive_rate   # fill_only_only
  + 1.0 * false_negative_rate   # depth_only
  + 1.0 * size_error
  + 1.0 * price_error
```

为什么 false positive 权重更高？

因为：

```text
fill_only_only = 回测里你以为成交了，但真实 LOB 不支持
```

这会直接虚增成交率和 PnL。

---

## 10. 不要试图完全消灭 `depth_only`

如果不加 LOB，你永远会遇到：

```text
LOB 有可见 ask/bid，所以 depth 模型能成交；
但是未来 trade tape 没有同方向成交，所以 fill-only NO_FILL。
```

这是 fill-only 的天然保守性。

比如 BUY-only 报告里的 `depth_only`：depth+fill 成交，但 fill-only 没成交。

如果你强行让 fill-only 也在这种情况下成交，你就只能引入：

```text
trade-inferred quote model
last trade mark model
pseudo-BBO model
```

这已经不是严格 fill-evidence execution，而是：

```text
Fill-only + inferred quote execution
```

可以作为一个独立 baseline，但不要混进主 fill-only 模型。

---

## 11. 修改后的模型分层

建议你把现在的 fill-only 拆成三档。

### A. `plumbing_replay`

用途：

```text
验证数据链路、真实成交复现。
```

规则：

```text
允许使用 source trade
participation_rate = 100%
price_buffer = 0
```

只能用于测试，不能作为回测主结果。

---

### B. `strict_fill_evidence`

用途：

```text
主审计结果 / 保守结果。
```

规则：

```text
exclude_source_trade = true
require_pre_arrival_quote_proxy = true
quote_proxy_ttl <= 5m/30m
min_trailing_same_side_trade_count >= 1/2
price_buffer >= 1 tick or 0.005
participation_rate <= 0.5% / 1%
TIF-sensitive short horizon
maker strict no-fill
```

这个应该显著减少 `fill_only_only`。

---

### C. `lob_holdout_calibrated_fill_only`

用途：

```text
主研究结果。
```

规则：

```text
使用 LOB holdout 离线校准的参数；
运行时只用 trade tape；
对每个 order 计算 p_depth_valid；
capacity = base_capacity * p_depth_valid；
或 p_depth_valid < threshold 时 reject。
```

示例：

```python
if p_depth_valid < 0.75:
    reject("lob_holdout_calibrated_low_validity_probability")

capacity = base_capacity * p_depth_valid
```

---

### D. `optimistic_trade_tape`

用途：

```text
上界，不做主结果。
```

这就是你现在更接近的模型：

```text
future same-side trade + limit + capacity
```

它可以保留，但报告必须标注：

```text
upper_bound / optimistic / not LOB-calibrated
```

---

## 12. 修改后的 taker 伪代码

```python
def fill_taker_fill_only(order, trade_index, config):
    if config.exclude_source_trade and order.signal_source_trade_id:
        excluded_trade_ids = {order.signal_source_trade_id}
    else:
        excluded_trade_ids = set()

    # 1. pre-arrival quote proxy
    if config.require_pre_arrival_quote_proxy:
        quote = trade_index.last_same_side_trade_before(
            asset_id=order.asset_id,
            side=order.side,
            ts=order.arrival_ts,
            ttl=config.quote_proxy_ttl,
        )

        if quote is None:
            return no_fill(order, "missing_pre_arrival_trade_quote_proxy")

        if order.side == "BUY" and quote.price > order.limit_price:
            return no_fill(order, "pre_arrival_pseudo_ask_above_limit")

        if order.side == "SELL" and quote.price < order.limit_price:
            return no_fill(order, "pre_arrival_pseudo_bid_below_limit")

    # 2. trailing activity gate
    trailing = trade_index.trailing_stats(
        asset_id=order.asset_id,
        side=order.side,
        end_ts=order.arrival_ts,
        lookback=config.trailing_lookback,
    )

    if trailing.count < config.min_trailing_trade_count:
        return no_fill(order, "insufficient_trailing_trade_count")

    if trailing.volume < config.min_trailing_volume:
        return no_fill(order, "insufficient_trailing_trade_volume")

    # 3. future eligible trades
    future_trades = trade_index.future_trades(
        asset_id=order.asset_id,
        side=order.side,
        start_ts=order.arrival_ts + config.min_post_arrival_delay,
        end_ts=order.deadline_ts,
        exclude_trade_ids=excluded_trade_ids,
    )

    remaining = order.size
    fills = []

    for trade in future_trades:
        if order.side == "BUY":
            if trade.price > order.limit_price:
                continue
            exec_price = trade.price + config.price_buffer(order, trade)
            if exec_price > order.limit_price:
                continue
        else:
            if trade.price < order.limit_price:
                continue
            exec_price = trade.price - config.price_buffer(order, trade)
            if exec_price < order.limit_price:
                continue

        base_capacity = trade.remaining_capacity * config.future_participation_rate
        trailing_cap = trailing.volume * config.trailing_participation_rate
        capacity = min(base_capacity, trailing_cap, config.absolute_order_cap)

        if config.validity_model:
            p = config.validity_model.predict(order, trade, trailing)
            if p < config.validity_threshold:
                continue
            capacity *= p

        qty = min(remaining, capacity)
        if qty <= 0:
            continue

        fills.append(fill(order, trade, exec_price, qty))
        remaining -= qty

        if remaining <= 0:
            break

    if not fills:
        return no_fill(order, "no_valid_fill_trade_after_gates")

    return result(order, fills, remaining)
```

---

## 13. 你下一步应该怎么做

按这个顺序改。

### Step 1：把 holdout verdict 改成分类指标

现在报告有 `fill_only_only`、`depth_only`、`both_filled_same`，很好。下一步输出：

```text
false_positive_rate = fill_only_only / samples
false_negative_rate = depth_only / samples
precision = both_filled / (both_filled + fill_only_only)
recall = both_filled / (both_filled + depth_only)
size_error
price_error
```

当前 order-level：

```text
samples = 9
both_filled_same = 4
fill_only_only = 5
precision ≈ 44.44%
```

这说明 fill-only 主模型需要收紧。

### Step 2：先加 `exclude_source_trade_id`

这是最该先做的。

重新跑 holdout，看 `fill_only_only` 是否下降。

### Step 3：加 `price_buffer >= 1 tick / 0.005`

重新跑 holdout。
如果 `fill_only_only` 大幅下降，说明之前主要是裸 trade price 过乐观。

### Step 4：加 pre-arrival quote proxy gate

重新跑 holdout。
这会进一步压低孤立成交造成的 false positive。

### Step 5：加 trade-density / trailing volume cap

重新跑 holdout。
看 precision 是否提升。

### Step 6：用 LOB holdout 校准参数

对不同组合跑 grid：

```text
quote_ttl
min_trailing_count
min_trailing_volume
price_buffer
participation_rate
horizon
```

选一个 false positive 最低、但成交率仍可接受的主配置。

---

## 14. 最后怎么解释这个模型

你以后报告里要这样写：

```text
Fill-only model v1:
    trade evidence exists -> participation fill
    optimistic upper bound

Fill-only model v2:
    trade evidence + pre-arrival trade quote proxy + activity gate + worse-price buffer
    LOB-holdout calibrated conservative model

Depth+fill model:
    uses actual L2 book
    higher-fidelity where fresh LOB is available
```

这样你不会把 fill-only 和 depth 模型混成一个东西。

---

## 15. 最终结论

你现在的 holdout 报告说明：

```text
fill-only 基础规则没错；
但它把“未来出现真实成交”当成了过强的可成交证明；
在 LOB 对照下出现了较高 fill_only_only 假阳性。
```

在不添加 LOB 的前提下，应该这样改：

```text
1. 排除 source trade，避免自我填充 / 未来函数。
2. 加 pre-arrival trade quote proxy，要求成交前已有新鲜交易价格上下文。
3. 加 trade-density / trailing volume gate，拒绝孤立成交。
4. 加 worse-price buffer，不能按裸历史成交价成交。
5. 降低 participation，并用 trailing volume 限制容量。
6. 按 TIF 缩短 horizon，尤其 FAK/IOC 不能等很久。
7. 用 LOB holdout 离线校准参数，但运行时仍只用 fill tape。
8. 把旧模型降级成 optimistic upper bound，而不是主结果。
```

一句话：

**fill-only 模型要从“未来有同方向成交就能参与”改成“未来成交只是证据之一；还必须有成交前上下文、价格缓冲、容量约束和 LOB-holdout 校准”。**
