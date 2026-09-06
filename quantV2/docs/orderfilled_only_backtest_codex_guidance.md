# Polymarket `OrderFilled-only` 回测框架 Codex 编程指导文档

> 目标读者：Codex / 开发者  
> 目标场景：历史 L2 LOB 数据不完整、不能做完整 DEPTH / queue replay，但链上 `OrderFilled` 数据相对完整。  
> 核心目标：从 `OrderFilled` 数据处理开始，构建一个严肃、可审计、保守、可解释的 `OrderFilled-only Trade-Tape Execution Model`，并生成 block/time 级价格摘要、执行成交模型、报告和验证体系。  
> 重要边界：这不是 L2 DEPTH 回测，不是 L3 FIFO 回测，也不是完整微观结构仿真。它是 **fill-evidence constrained execution**：策略订单只能合理参与历史真实发生过的成交流的一小部分。

---

## 0. 总原则

当 LOB 不齐全时，不能继续假装自己知道当时的 bid/ask、depth、spread、queue、撤单和未成交订单。因此，本模型必须从下面这个问题出发：

```text
我的策略订单是否能够合理参与历史上真实发生过的 OrderFilled 成交流？
```

而不是：

```text
我的订单当时是否能在完整盘口上成交？
```

所以本框架必须遵守：

```text
1. 不虚构盘口流动性。
2. 不用 stale LOB。
3. 不用 block close / OHLCV 直接成交。
4. 不把 high/low/touched price 当作可成交。
5. 不简单 sum raw OrderFilled 当成交量。
6. 不允许模拟成交量超过历史真实成交容量上限。
7. 每笔模拟 fill 都必须能追溯到 source OrderFilled / trade print。
8. 报告必须展示 capacity、latency、price buffer、participation sensitivity。
```

推荐模型名称：

```text
OrderFilledOnlyTradeTapeExecutionModel
OrderFilledReplayExecutionModel
FillEvidenceExecutionModel
```

禁止命名为：

```text
L2DepthExecutionModel
L3ExecutionModel
TrueFIFOExecutionModel
```

---

## 1. 目标产物

Codex 需要实现以下数据管线和执行模块：

```text
raw_orderfilled
    ↓ normalize side / price / size / asset_id
maker_fill_ticks
    ↓ dedupe + group + one-sided volume
trade_prints_one_sided
    ↓
    ├── block_trade_bars_sparse
    ├── block_marks_dense
    ├── time_bars_1m_5m
    ├── last_trade_marks
    └── orderfilled_only_execution_engine
```

执行引擎只读：

```text
trade_prints_one_sided
```

信号层可以读：

```text
block_trade_bars_sparse
time_bars_1m_5m
block_marks_dense with stale control
last_trade_marks
```

估值层可以读：

```text
last_trade_mark
settlement mark
stale-aware mark
```

执行层绝对不能直接读：

```text
raw_orderfilled
maker_fill_ticks
block OHLCV
block_marks_dense
last price carry-forward
```

---

## 2. 数据表设计

### 2.1 `raw_orderfilled`

这是原始链上事件表。只做解析、去重、类型规范，不做交易语义解释。

#### 必需字段

```sql
CREATE TABLE raw_orderfilled (
    chain_id                    BIGINT NOT NULL,
    contract_address             TEXT NOT NULL,
    block_number                 BIGINT NOT NULL,
    block_time                   TIMESTAMPTZ NOT NULL,
    tx_hash                      TEXT NOT NULL,
    tx_index                     BIGINT,
    log_index                    BIGINT NOT NULL,
    order_hash                   TEXT NOT NULL,
    maker                        TEXT NOT NULL,
    taker                        TEXT NOT NULL,
    maker_asset_id               TEXT NOT NULL,
    taker_asset_id               TEXT NOT NULL,
    maker_amount_filled_raw      NUMERIC NOT NULL,
    taker_amount_filled_raw      NUMERIC NOT NULL,
    fee_raw                      NUMERIC,
    raw_json                     JSONB,
    inserted_at                  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (chain_id, tx_hash, log_index)
);
```

#### 用途

```text
1. 审计。
2. 回溯。
3. 重新 normalize。
4. 排查异常方向、异常价格、重复事件。
```

#### 不允许

```text
不要直接用 raw_orderfilled 作为策略信号或成交执行输入。
不要直接 sum raw_orderfilled 得到 volume。
```

---

### 2.2 `market_asset_map`

`OrderFilled` 里主要是 asset id。策略和报告需要 market / condition / outcome 语义。因此必须有资产映射表。

```sql
CREATE TABLE market_asset_map (
    chain_id            BIGINT NOT NULL,
    market_id           TEXT NOT NULL,
    condition_id        TEXT NOT NULL,
    asset_id            TEXT NOT NULL,
    outcome             TEXT NOT NULL, -- YES / NO / custom outcome
    canonical_side      TEXT,          -- YES / NO
    token_decimals      INT NOT NULL DEFAULT 6,
    usdc_decimals       INT NOT NULL DEFAULT 6,
    tick_size           NUMERIC,
    min_order_size      NUMERIC,
    market_slug         TEXT,
    market_title        TEXT,
    category            TEXT,
    start_time          TIMESTAMPTZ,
    close_time          TIMESTAMPTZ,
    resolution_time     TIMESTAMPTZ,
    winner_asset_id     TEXT,
    active              BOOLEAN,
    closed              BOOLEAN,
    metadata_source     TEXT,
    updated_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (chain_id, asset_id)
);
```

#### 验收规则

```text
1. 每个 maker_fill_ticks.asset_id 必须能映射到 market_id / condition_id。
2. 无法映射的 OrderFilled 进入 quarantine，不进入执行流。
3. YES/NO token 不要混用。
4. 如果生成 canonical YES price，则单独生成研究表，不要覆盖原始 asset price。
```

---

### 2.3 `maker_fill_ticks`

这是从每一条 `OrderFilled` 规范化出来的 maker-level fill。它保留每个 maker order 被成交的细节，适合做链上审计、maker 侧分析、钱包复盘，但它还不一定是最终的市场成交 tape。

```sql
CREATE TABLE maker_fill_ticks (
    fill_id                 TEXT PRIMARY KEY,
    chain_id                BIGINT NOT NULL,
    block_number            BIGINT NOT NULL,
    block_time              TIMESTAMPTZ NOT NULL,
    tx_hash                 TEXT NOT NULL,
    tx_index                BIGINT,
    log_index               BIGINT NOT NULL,
    order_hash              TEXT NOT NULL,

    market_id               TEXT NOT NULL,
    condition_id            TEXT NOT NULL,
    asset_id                TEXT NOT NULL,
    outcome                 TEXT NOT NULL,

    price                   NUMERIC NOT NULL,
    size_shares             NUMERIC NOT NULL,
    notional_usdc           NUMERIC NOT NULL,

    passive_side            TEXT NOT NULL, -- BUY / SELL, maker order side
    aggressor_side          TEXT NOT NULL, -- BUY / SELL, taker/aggressor side

    maker                   TEXT NOT NULL,
    taker                   TEXT NOT NULL,
    fee_usdc                NUMERIC DEFAULT 0,

    source                  TEXT NOT NULL DEFAULT 'orderfilled',
    confidence              TEXT NOT NULL DEFAULT 'high',
    created_at              TIMESTAMPTZ DEFAULT now(),

    UNIQUE (chain_id, tx_hash, log_index)
);
```

#### 方向和价格规范化规则

设 `USDC_ASSET_ID = 0`。

```text
Case A: makerAssetId == 0
    maker 给出 USDC，收到 outcome token。
    maker 是被动买方。

    passive_side   = BUY
    aggressor_side = SELL
    asset_id       = takerAssetId
    size_shares    = normalize_token_amount(takerAmountFilled)
    notional_usdc  = normalize_usdc_amount(makerAmountFilled)
    price          = notional_usdc / size_shares

Case B: takerAssetId == 0
    maker 给出 outcome token，收到 USDC。
    maker 是被动卖方。

    passive_side   = SELL
    aggressor_side = BUY
    asset_id       = makerAssetId
    size_shares    = normalize_token_amount(makerAmountFilled)
    notional_usdc  = normalize_usdc_amount(takerAmountFilled)
    price          = notional_usdc / size_shares

Case C: makerAssetId != 0 and takerAssetId != 0
    复杂路径 / 组合路径 / 不支持路径。
    进入 quarantine。
```

#### 必须检查

```text
price > 0
price <= 1
size_shares > 0
notional_usdc > 0
asset_id exists in market_asset_map
passive_side in {BUY, SELL}
aggressor_side in {BUY, SELL}
```

任何失败都进入：

```text
orderfilled_quarantine
```

---

### 2.4 `orderfilled_quarantine`

```sql
CREATE TABLE orderfilled_quarantine (
    chain_id            BIGINT,
    tx_hash             TEXT,
    log_index           BIGINT,
    order_hash          TEXT,
    reason              TEXT NOT NULL,
    raw_json            JSONB,
    created_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (chain_id, tx_hash, log_index)
);
```

常见原因：

```text
unknown_asset_id
unsupported_asset_pair
zero_size
zero_notional
price_out_of_range
duplicate_event
missing_block_time
negative_amount
```

---

### 2.5 `trade_prints_one_sided`

这是 `OrderFilled-only` 回测执行层的主数据。它的目标是把 maker-level fills 聚合成不会双算的、可作为历史市场成交流的 trade tape。

```sql
CREATE TABLE trade_prints_one_sided (
    trade_id                    TEXT PRIMARY KEY,
    chain_id                    BIGINT NOT NULL,
    block_number                BIGINT NOT NULL,
    block_time                  TIMESTAMPTZ NOT NULL,
    tx_hash                     TEXT NOT NULL,
    tx_index                    BIGINT,
    trade_group_id              TEXT NOT NULL,

    market_id                   TEXT NOT NULL,
    condition_id                TEXT NOT NULL,
    asset_id                    TEXT NOT NULL,
    outcome                     TEXT NOT NULL,

    price                       NUMERIC NOT NULL,
    size_shares                 NUMERIC NOT NULL,
    notional_usdc               NUMERIC NOT NULL,

    aggressor_side              TEXT NOT NULL, -- BUY / SELL
    passive_side                TEXT NOT NULL, -- SELL / BUY

    source_fill_ids             TEXT[] NOT NULL,
    source_order_hashes          TEXT[],
    source_log_indexes           BIGINT[],
    confidence                  TEXT NOT NULL DEFAULT 'medium',
    created_at                  TIMESTAMPTZ DEFAULT now()
);
```

#### 聚合规则 v1

如果暂时没有 `OrdersMatched` 事件，先按以下 key 聚合：

```text
chain_id
block_number
tx_hash
asset_id
aggressor_side
passive_side
price
```

得到一条 `trade_print`。

```text
size_shares   = sum(size_shares)
notional_usdc = sum(notional_usdc)
source_fill_ids = collect(fill_id)
source_log_indexes = collect(log_index sorted)
```

#### 为什么不能直接 sum raw OrderFilled

```text
1. 一个 tx 内可能有多个 maker fills。
2. 一个 taker sweep 可能扫多个 maker order。
3. 如果直接 sum raw OrderFilled，可能重复计算成交量。
4. 交易容量模型应该使用 one-sided market tape，而不是 maker-level raw 行数。
```

#### 同一 tx 多价格处理

如果同一个 tx 内 sweep 多个价格：

```text
BUY 100 @ 0.51
BUY 200 @ 0.52
BUY 300 @ 0.53
```

要保留三条 price-level trade prints，不要合并成：

```text
BUY 600 @ 0.52 VWAP
```

因为执行层需要知道 limit price 能否参与每个价位。

---

## 3. 价格数据产品

价格数据必须分用途。执行价、信号价、估值价不能混用。

---

### 3.1 执行价格

执行层使用：

```text
trade_prints_one_sided.price
```

但不代表订单可以全部按这个价格成交。订单只能在满足以下条件时参与真实历史 trade print 的一小部分：

```text
1. trade.block_time >= order.arrival_ts
2. trade.block_time <= order.arrival_ts + execution_horizon
3. trade.aggressor_side 与订单方向一致，或对 maker 是相反成交方向
4. trade.price 满足 limit price
5. trade 剩余可分配 capacity > 0
6. 执行价格经过 worse-price buffer 后仍满足 limit
```

---

### 3.2 信号价格

策略信号可以使用：

```text
last_trade_price
block_trade_bars_sparse
time_bars_1m_5m
VWAP
成交量
成交方向 imbalance
价格动量
成交密度
```

但这些只能用于：

```text
signal
feature
research
valuation
plot
```

不能用于：

```text
execution fill
```

---

### 3.3 估值价格

没有 LOB 时，没有 mid price，也没有 best bid / ask。组合估值只能用：

```text
last_trade_price
last_valid_trade_price
settlement value
canonical YES mark
```

但必须带 stale 标记：

```sql
CREATE TABLE last_trade_marks (
    chain_id                BIGINT NOT NULL,
    market_id               TEXT NOT NULL,
    asset_id                TEXT NOT NULL,
    asof_block_number       BIGINT NOT NULL,
    asof_block_time         TIMESTAMPTZ NOT NULL,
    last_trade_id           TEXT,
    last_trade_price        NUMERIC,
    last_trade_block_number BIGINT,
    last_trade_block_time   TIMESTAMPTZ,
    price_age_blocks        BIGINT,
    price_age_seconds       BIGINT,
    is_stale_price          BOOLEAN NOT NULL,
    mark_source             TEXT NOT NULL, -- last_trade / settlement / fallback
    PRIMARY KEY (chain_id, market_id, asset_id, asof_block_number)
);
```

如果 last trade 是 6 小时前的 0.62，则可以用于临时 mark，但必须：

```text
mark_source = last_trade
is_stale_price = true
price_age_seconds = 21600
```

不要把 stale last price 当成可成交价格。

---

## 4. Block 级和时间级 OHLCV

必须做 blocknumber 级 OHLCV，但它只属于摘要层 / 信号层 / 估值层，不属于执行层。

---

### 4.1 `block_trade_bars_sparse`

只在有真实成交的 block 生成 bar。

```sql
CREATE TABLE block_trade_bars_sparse (
    chain_id                BIGINT NOT NULL,
    market_id               TEXT NOT NULL,
    condition_id            TEXT NOT NULL,
    asset_id                TEXT NOT NULL,
    outcome                 TEXT NOT NULL,
    block_number            BIGINT NOT NULL,
    block_time_start        TIMESTAMPTZ NOT NULL,
    block_time_end          TIMESTAMPTZ NOT NULL,

    open_price              NUMERIC NOT NULL,
    high_price              NUMERIC NOT NULL,
    low_price               NUMERIC NOT NULL,
    close_price             NUMERIC NOT NULL,
    vwap_price              NUMERIC NOT NULL,

    volume_shares           NUMERIC NOT NULL,
    notional_usdc           NUMERIC NOT NULL,
    trade_count             BIGINT NOT NULL,

    buy_volume_shares       NUMERIC NOT NULL,
    sell_volume_shares      NUMERIC NOT NULL,
    buy_notional_usdc       NUMERIC NOT NULL,
    sell_notional_usdc      NUMERIC NOT NULL,
    buy_count               BIGINT NOT NULL,
    sell_count              BIGINT NOT NULL,

    first_trade_id          TEXT NOT NULL,
    last_trade_id           TEXT NOT NULL,

    source                  TEXT NOT NULL DEFAULT 'orderfilled_one_sided',
    is_sparse               BOOLEAN NOT NULL DEFAULT true,

    PRIMARY KEY (chain_id, market_id, asset_id, block_number)
);
```

#### 聚合规则

对每个：

```text
chain_id + market_id + asset_id + block_number
```

按：

```text
block_number ASC, tx_index ASC NULLS LAST, tx_hash ASC, source_log_indexes ASC
```

排序。

```text
open  = 第一笔 trade price
high  = 最高 trade price
low   = 最低 trade price
close = 最后一笔 trade price
volume = sum(size_shares)
notional = sum(notional_usdc)
vwap = notional / volume
buy_volume = sum(size where aggressor_side = BUY)
sell_volume = sum(size where aggressor_side = SELL)
```

注意：

```text
volume 必须来自 trade_prints_one_sided，不要来自 raw_orderfilled sum。
```

---

### 4.2 `block_marks_dense`

如果策略引擎或图表需要每个 block 都有 mark，可以生成稠密表。

```sql
CREATE TABLE block_marks_dense (
    chain_id                    BIGINT NOT NULL,
    market_id                   TEXT NOT NULL,
    condition_id                TEXT NOT NULL,
    asset_id                    TEXT NOT NULL,
    outcome                     TEXT NOT NULL,
    block_number                BIGINT NOT NULL,
    block_time                  TIMESTAMPTZ NOT NULL,

    mark_price                  NUMERIC,
    open_price                  NUMERIC,
    high_price                  NUMERIC,
    low_price                   NUMERIC,
    close_price                 NUMERIC,
    vwap_price                  NUMERIC,

    volume_shares               NUMERIC NOT NULL DEFAULT 0,
    notional_usdc               NUMERIC NOT NULL DEFAULT 0,
    trade_count                 BIGINT NOT NULL DEFAULT 0,

    is_trade_observed           BOOLEAN NOT NULL,
    is_carry_forward            BOOLEAN NOT NULL,
    is_stale_price              BOOLEAN NOT NULL,
    price_age_blocks            BIGINT,
    price_age_seconds           BIGINT,
    last_trade_id               TEXT,

    PRIMARY KEY (chain_id, market_id, asset_id, block_number)
);
```

无成交 block：

```text
volume_shares = 0
notional_usdc = 0
trade_count = 0
is_trade_observed = false
is_carry_forward = true
mark_price = previous close / last_trade_price
is_stale_price = price_age > stale_threshold
```

强制规则：

```text
block_marks_dense 不允许进入执行引擎。
```

---

### 4.3 `time_bars_1m_5m`

从 `trade_prints_one_sided` 或 `block_trade_bars_sparse` 聚合。

```sql
CREATE TABLE time_bars (
    chain_id                BIGINT NOT NULL,
    market_id               TEXT NOT NULL,
    condition_id            TEXT NOT NULL,
    asset_id                TEXT NOT NULL,
    outcome                 TEXT NOT NULL,
    interval                TEXT NOT NULL, -- 1m / 5m / 15m / 1h
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,

    open_price              NUMERIC,
    high_price              NUMERIC,
    low_price               NUMERIC,
    close_price             NUMERIC,
    vwap_price              NUMERIC,

    volume_shares           NUMERIC NOT NULL DEFAULT 0,
    notional_usdc           NUMERIC NOT NULL DEFAULT 0,
    trade_count             BIGINT NOT NULL DEFAULT 0,
    buy_volume_shares       NUMERIC NOT NULL DEFAULT 0,
    sell_volume_shares      NUMERIC NOT NULL DEFAULT 0,

    is_carry_forward        BOOLEAN NOT NULL DEFAULT false,
    is_stale_price          BOOLEAN NOT NULL DEFAULT false,
    price_age_seconds       BIGINT,

    PRIMARY KEY (chain_id, market_id, asset_id, interval, window_start)
);
```

---

### 4.4 `canonical_yes_block_bars`

如果需要把 NO 价格转为 YES 概率，可以单独建研究表：

```text
YES token trade price -> yes_price = price
NO token trade price  -> yes_price = 1 - price
```

注意：

```text
canonical YES bars 只能做研究 / 信号 / 图表，不能做执行。
执行必须回到具体 asset_id 的 trade_prints_one_sided。
```

---

## 5. 执行模型总览

### 5.1 模型等级

本框架至少支持三个模式：

```text
strict_audit
conservative_trade_tape
optimistic_sensitivity
```

#### `strict_audit`

```text
Taker:
    只允许参与 arrival_ts 之后的同方向真实成交。
    participation_rate = 0.5% ~ 2.5%。
    价格加 1 tick 或更大 buffer。
    没有真实成交就不成交。

Maker:
    默认不进入主 PnL。
    只记录 candidate fills。
```

#### `conservative_trade_tape`

```text
Taker:
    参与 arrival_ts 之后的同方向真实成交。
    participation_rate = 1% ~ 2.5%。
    加 price buffer。
    可以跨多个 trade prints 慢慢填满。

Maker:
    使用 phantom_queue。
    phantom_queue 清零后才允许 fill。
    maker fill 也受 participation cap 限制。
```

#### `optimistic_sensitivity`

```text
Taker:
    participation_rate = 5%。
    buffer 较小。
    execution_horizon 更长。

Maker:
    phantom_queue 更小。
    仅作为上界。
```

禁止把 optimistic 作为唯一主结果。

---

### 5.2 配置对象

```python
@dataclass(frozen=True)
class OrderFilledOnlyExecutionConfig:
    mode: Literal["strict_audit", "conservative_trade_tape", "optimistic_sensitivity"]

    participation_rate: Decimal
    maker_participation_rate: Decimal

    latency_ms: int
    execution_horizon_ms: int

    tick_size: Decimal
    taker_price_buffer_ticks: int
    taker_price_buffer_abs: Decimal
    use_square_impact: bool
    impact_coefficient: Decimal

    maker_mode: Literal["strict_no_fill", "phantom_queue"]
    fixed_queue_floor_shares: Decimal
    queue_volume_multiplier: Decimal
    queue_notional_floor_usdc: Decimal

    allow_price_through_for_maker: bool
    allow_same_block_fill_after_signal: bool

    stale_price_threshold_seconds: int
```

推荐默认：

```python
STRICT_AUDIT = OrderFilledOnlyExecutionConfig(
    mode="strict_audit",
    participation_rate=Decimal("0.01"),
    maker_participation_rate=Decimal("0"),
    latency_ms=1000,
    execution_horizon_ms=5 * 60 * 1000,
    tick_size=Decimal("0.01"),
    taker_price_buffer_ticks=1,
    taker_price_buffer_abs=Decimal("0"),
    use_square_impact=False,
    impact_coefficient=Decimal("0"),
    maker_mode="strict_no_fill",
    fixed_queue_floor_shares=Decimal("0"),
    queue_volume_multiplier=Decimal("0"),
    queue_notional_floor_usdc=Decimal("0"),
    allow_price_through_for_maker=False,
    allow_same_block_fill_after_signal=False,
    stale_price_threshold_seconds=30 * 60,
)
```

---

## 6. 订单模型

```python
@dataclass(frozen=True)
class StrategyOrderIntent:
    order_id: str
    signal_ts: datetime
    signal_block_number: int | None
    market_id: str
    condition_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]
    order_kind: Literal["TAKER", "MAKER", "LIMIT"]
    limit_price: Decimal
    size_shares: Decimal
    tif: Literal["GTC", "GTD", "FOK", "FAK", "IOC"]
    post_only: bool
    expires_at: datetime | None
```

执行时计算：

```text
arrival_ts = signal_ts + latency_ms
```

如果只有 block 粒度：

```text
arrival_block_number = signal_block_number + latency_blocks
```

默认不要允许同 block 未来 log 提前成交，除非有 tx_index/log_index 能严格证明策略订单在这些历史成交之前已经可见。

---

## 7. Taker 执行模型

### 7.1 核心规则

BUY taker：

```text
1. trade.aggressor_side == BUY
2. trade.price <= order.limit_price before buffer
3. exec_price = trade.price + adverse_buffer
4. exec_price <= order.limit_price
5. trade.ts >= order.arrival_ts
6. trade.ts <= order.arrival_ts + execution_horizon
7. allocated_size <= remaining_trade_capacity
```

SELL taker：

```text
1. trade.aggressor_side == SELL
2. trade.price >= order.limit_price before buffer
3. exec_price = trade.price - adverse_buffer
4. exec_price >= order.limit_price
5. trade.ts >= order.arrival_ts
6. trade.ts <= order.arrival_ts + execution_horizon
7. allocated_size <= remaining_trade_capacity
```

没有合格 trade print：

```text
不成交。
```

不能使用：

```text
block close
last price
previous block price
carry-forward mark
OHLCV high/low
```

---

### 7.2 Capacity ledger

每条 trade print 有可消耗容量：

```text
max_capacity = trade.size_shares * participation_rate
remaining_capacity = max_capacity - already_allocated_by_simulated_orders
```

表：

```sql
CREATE TABLE trade_capacity_ledger (
    run_id                  TEXT NOT NULL,
    trade_id                TEXT NOT NULL,
    max_capacity_shares     NUMERIC NOT NULL,
    allocated_shares        NUMERIC NOT NULL DEFAULT 0,
    remaining_capacity      NUMERIC NOT NULL,
    participation_rate      NUMERIC NOT NULL,
    PRIMARY KEY (run_id, trade_id)
);
```

强制：

```text
同一 backtest run 内，多个模拟订单不能重复消耗同一条 trade print 的 capacity。
```

---

### 7.3 Taker 伪代码

```python
def fill_taker_orderfilled_only(order, trade_tape, capacity_ledger, config):
    arrival_ts = order.signal_ts + timedelta(milliseconds=config.latency_ms)
    deadline = arrival_ts + timedelta(milliseconds=config.execution_horizon_ms)
    remaining = order.size_shares
    fills = []

    trades = trade_tape.query(
        market_id=order.market_id,
        asset_id=order.asset_id,
        ts_start=arrival_ts,
        ts_end=deadline,
        sort_by=["block_number", "tx_index", "tx_hash", "trade_id"],
    )

    for trade in trades:
        if order.side == "BUY":
            if trade.aggressor_side != "BUY":
                continue
            if trade.price > order.limit_price:
                continue
            buffer = calculate_taker_buffer(order, trade, config)
            exec_price = trade.price + buffer
            if exec_price > order.limit_price:
                continue

        elif order.side == "SELL":
            if trade.aggressor_side != "SELL":
                continue
            if trade.price < order.limit_price:
                continue
            buffer = calculate_taker_buffer(order, trade, config)
            exec_price = trade.price - buffer
            if exec_price < order.limit_price:
                continue

        available = capacity_ledger.remaining_capacity(trade.trade_id)
        qty = min(remaining, available)
        if qty <= 0:
            continue

        fills.append(ExecutionFill(
            order_id=order.order_id,
            trade_id=trade.trade_id,
            fill_ts=trade.block_time,
            side=order.side,
            price=exec_price,
            size_shares=qty,
            liquidity_flag="TAKER",
            source_trade_ids=[trade.trade_id],
            reason="orderfilled_trade_tape_capacity",
        ))

        capacity_ledger.consume(trade.trade_id, qty)
        remaining -= qty

        if remaining <= 0:
            break

    return fills, remaining
```

---

### 7.4 TIF 处理

```text
FOK:
    先 dry-run 计算是否可全量成交。
    如果不能全量成交，0 fill，REJECTED。

FAK / IOC:
    成交可成交部分，剩余 CANCELLED。

GTC / GTD:
    OrderFilled-only 模型下，taker 剩余部分不能凭空挂 book。
    如果没有 LOB，剩余部分默认 CANCELLED 或转入 maker phantom model，但必须单独标记。
```

推荐：

```text
OrderFilled-only 主结果：GTC 剩余不自动转 maker。
只有显式 maker 策略才进入 phantom_queue maker 模型。
```

---

## 8. Maker 执行模型

### 8.1 默认立场

没有 LOB 时，maker 策略最容易被高估。因此主结果建议：

```text
maker_mode = strict_no_fill
```

也就是：

```text
记录 maker candidate，但不计入 PnL。
```

可选的敏感性模型：

```text
maker_mode = phantom_queue
```

---

### 8.2 Phantom queue

没有 LOB 时，不知道前方 queue，所以定义虚拟前方队列：

```text
phantom_queue = max(
    fixed_queue_floor_shares,
    queue_volume_multiplier * trailing_opposite_flow_at_price,
    queue_notional_floor_usdc / price
)
```

`trailing_opposite_flow_at_price` 示例：

```text
BUY maker @ P:
    取 arrival_ts 前 lookback window 内 aggressor_side=SELL 且 price<=P 的成交量。

SELL maker @ P:
    取 arrival_ts 前 lookback window 内 aggressor_side=BUY 且 price>=P 的成交量。
```

---

### 8.3 Maker BUY

BUY maker order：

```text
BUY @ P
```

只能被以下 trade 推进：

```text
trade.aggressor_side == SELL
trade.price <= P
trade.ts >= arrival_ts
```

流程：

```text
1. trade flow 先消耗 phantom_queue。
2. phantom_queue 清零后，剩余 flow 才可 fill 自己。
3. fill 也受 maker_participation_rate 限制。
4. maker fill price = order.limit_price。
```

---

### 8.4 Maker SELL

SELL maker order：

```text
SELL @ P
```

只能被以下 trade 推进：

```text
trade.aggressor_side == BUY
trade.price >= P
trade.ts >= arrival_ts
```

流程同 BUY maker。

---

### 8.5 Maker 伪代码

```python
def admit_maker_orderfilled_only(order, trade_tape, config):
    if config.maker_mode == "strict_no_fill":
        return WorkingMakerOrder(
            order=order,
            state="WORKING_BUT_NON_EXECUTABLE_IN_ORDERFILLED_ONLY",
            phantom_queue=None,
            included_in_pnl=False,
        )

    phantom_queue = estimate_phantom_queue(order, trade_tape, config)
    return WorkingMakerOrder(
        order=order,
        state="WORKING",
        phantom_queue=phantom_queue,
        included_in_pnl=True,
    )


def process_trade_for_maker_order(order, trade, capacity_ledger, config):
    if order.side == "BUY":
        if trade.aggressor_side != "SELL":
            return []
        if trade.price > order.limit_price:
            return []
        exec_price = order.limit_price

    elif order.side == "SELL":
        if trade.aggressor_side != "BUY":
            return []
        if trade.price < order.limit_price:
            return []
        exec_price = order.limit_price

    available_flow = capacity_ledger.remaining_capacity(trade.trade_id, rate=config.maker_participation_rate)
    if available_flow <= 0:
        return []

    if order.phantom_queue > 0:
        consumed = min(order.phantom_queue, available_flow)
        order.phantom_queue -= consumed
        capacity_ledger.consume(trade.trade_id, consumed, queue_consumption=True)
        available_flow -= consumed

    if available_flow <= 0:
        return []

    qty = min(order.remaining_size, available_flow)
    fill = ExecutionFill(
        order_id=order.order_id,
        trade_id=trade.trade_id,
        fill_ts=trade.block_time,
        side=order.side,
        price=exec_price,
        size_shares=qty,
        liquidity_flag="MAKER",
        source_trade_ids=[trade.trade_id],
        reason="phantom_queue_cleared_by_orderfilled_trade",
    )
    order.remaining_size -= qty
    capacity_ledger.consume(trade.trade_id, qty)
    return [fill]
```

---

## 9. Worse-price rule

没有 LOB 时，不能给策略价格改善。

### BUY taker

```text
base_price = historical_trade_price
exec_price = base_price + buffer
exec_price <= limit_price
```

### SELL taker

```text
base_price = historical_trade_price
exec_price = base_price - buffer
exec_price >= limit_price
```

### Maker

```text
exec_price = order.limit_price
```

### Buffer 计算

第一版：

```python
def calculate_taker_buffer(order, trade, config):
    tick_buffer = config.tick_size * config.taker_price_buffer_ticks
    abs_buffer = config.taker_price_buffer_abs
    return max(tick_buffer, abs_buffer)
```

第二版可以加入平方 impact：

```text
impact = impact_coefficient * (allocated_size / eligible_volume) ** 2 * trade.price
```

---

## 10. Look-ahead 控制

必须强制：

```text
1. 策略只能看到 signal_ts 之前的数据。
2. 订单只能在 arrival_ts 之后参与成交。
3. arrival_ts = signal_ts + latency。
4. 如果只有 block_time，则 OrderFilled 只能在 block_time 之后生效。
5. 不允许用未来 block 的 close/high/low 生成当前订单。
6. 不允许同 block 后续 log 被当前策略看见，除非你有严格的 tx/log 顺序模拟。
```

建议默认：

```text
allow_same_block_fill_after_signal = false
```

如果需要 block 内成交，必须具备：

```text
tx_index
log_index
strategy_event_order
```

否则宁可保守延迟到下一个 block。

---

## 11. Execution audit log

每笔模拟订单必须输出审计记录。

```sql
CREATE TABLE execution_audit_log (
    run_id                      TEXT NOT NULL,
    order_id                    TEXT NOT NULL,
    model                       TEXT NOT NULL,
    signal_ts                   TIMESTAMPTZ NOT NULL,
    arrival_ts                  TIMESTAMPTZ NOT NULL,
    market_id                   TEXT NOT NULL,
    asset_id                    TEXT NOT NULL,
    side                        TEXT NOT NULL,
    order_kind                  TEXT NOT NULL,
    limit_price                 NUMERIC NOT NULL,
    requested_size              NUMERIC NOT NULL,
    filled_size                 NUMERIC NOT NULL,
    avg_fill_price              NUMERIC,
    unfilled_size               NUMERIC NOT NULL,
    final_state                 TEXT NOT NULL,
    reason                      TEXT,
    participation_rate          NUMERIC,
    latency_ms                  BIGINT,
    price_buffer                NUMERIC,
    source_trade_ids            TEXT[],
    source_tx_hashes            TEXT[],
    audit_json                  JSONB,
    created_at                  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (run_id, order_id)
);
```

每个 fill 的 audit JSON 至少包括：

```json
{
  "trade_id": "...",
  "tx_hash": "...",
  "historical_aggressor_side": "BUY",
  "historical_price": "0.52",
  "historical_size": "2000",
  "participation_rate": "0.025",
  "allocated_size": "50",
  "base_price": "0.52",
  "price_buffer": "0.01",
  "exec_price": "0.53",
  "capacity_before": "50",
  "capacity_after": "0"
}
```

最终报告必须能回答：

```text
为什么成交？
对应哪条 OrderFilled-derived trade print？
tx_hash 是什么？
历史成交方向是什么？
历史成交价格是多少？
历史成交量是多少？
我的 participation cap 是多少？
成交价为什么是这个？
有没有用未来数据？
为什么没完全成交？
```

---

## 12. Codex 开发任务拆分

### Task 1：数据类型

文件：

```text
src/backtest/orderfilled_only/types.py
```

实现：

```python
RawOrderFilled
MakerFillTick
TradePrint
BlockTradeBar
BlockMark
TimeBar
StrategyOrderIntent
ExecutionFill
ExecutionAudit
OrderFilledOnlyExecutionConfig
```

要求：

```text
价格、size、notional 用 Decimal。
所有对象有 stable id。
所有对象可 JSON serialize。
所有 side 字段只能是 BUY / SELL。
```

---

### Task 2：OrderFilled normalizer

文件：

```text
src/backtest/orderfilled_only/normalizer.py
```

实现：

```python
normalize_raw_orderfilled(raw, asset_map) -> MakerFillTick | QuarantineRecord
```

验收：

```text
makerAssetId == 0 -> passive_side BUY, aggressor_side SELL。
takerAssetId == 0 -> passive_side SELL, aggressor_side BUY。
price 在 (0,1]。
size > 0。
asset_id 能映射到 market。
异常进入 quarantine。
```

---

### Task 3：One-sided trade print builder

文件：

```text
src/backtest/orderfilled_only/trade_print_builder.py
```

实现：

```python
build_trade_prints(maker_fill_ticks) -> list[TradePrint]
```

聚合 key：

```text
chain_id, block_number, tx_hash, asset_id, aggressor_side, passive_side, price
```

验收：

```text
同一 tx 同一 asset/side/price 的 maker fills 合并。
不同 price 不合并。
不同 aggressor_side 不合并。
source_fill_ids 完整保留。
size/notional 加总正确。
trade_id deterministic。
```

---

### Task 4：Block bar builder

文件：

```text
src/backtest/orderfilled_only/bars.py
```

实现：

```python
build_block_trade_bars_sparse(trade_prints)
build_block_marks_dense(block_trade_bars_sparse, block_range, stale_threshold)
build_time_bars(trade_prints, interval)
build_canonical_yes_bars(optional)
```

验收：

```text
open = block 内第一笔 trade price。
close = block 内最后一笔 trade price。
high/low 正确。
volume = one-sided size sum。
无成交 block 不出现在 sparse bars。
dense marks 的无成交 block volume=0, trade_count=0, is_carry_forward=true。
dense marks 不进入 execution engine。
```

---

### Task 5：Capacity ledger

文件：

```text
src/backtest/orderfilled_only/capacity.py
```

实现：

```python
class TradeCapacityLedger:
    initialize(trade_prints, participation_rate)
    remaining_capacity(trade_id)
    consume(trade_id, qty)
```

验收：

```text
allocated <= trade.size * participation_rate。
多个订单不能重复消耗同一 trade capacity。
consume 超额必须抛错。
ledger deterministic。
```

---

### Task 6：Taker execution

文件：

```text
src/backtest/orderfilled_only/taker.py
```

实现：

```python
fill_taker_orderfilled_only(order, trade_tape, ledger, config)
```

验收：

```text
BUY 只用 aggressor_side=BUY。
SELL 只用 aggressor_side=SELL。
BUY 不用 price > limit 的 trade。
SELL 不用 price < limit 的 trade。
fill_ts >= arrival_ts。
没有合格 trade 不成交。
容量不超过 participation cap。
```

---

### Task 7：Maker phantom execution

文件：

```text
src/backtest/orderfilled_only/maker.py
```

实现：

```python
admit_maker_orderfilled_only(order, trade_tape, config)
process_trade_for_maker_order(order, trade, ledger, config)
estimate_phantom_queue(order, trade_tape, config)
```

验收：

```text
strict_no_fill 模式 maker 不计入 PnL。
BUY maker 只被 SELL flow 推进。
SELL maker 只被 BUY flow 推进。
phantom_queue 先被消耗，清零后才 fill。
maker fill price = order.limit_price。
```

---

### Task 8：Execution engine

文件：

```text
src/backtest/orderfilled_only/engine.py
```

实现：

```python
run_orderfilled_only_backtest(strategy_orders, trade_prints, config)
```

职责：

```text
1. 按 signal_ts 排序策略订单。
2. 计算 arrival_ts。
3. 初始化 capacity ledger。
4. 路由 taker / maker。
5. 处理 TIF。
6. 生成 ExecutionFill。
7. 生成 ExecutionAudit。
8. 输出 portfolio events。
```

验收：

```text
同样输入重复运行结果一致。
无 look-ahead。
无容量重复消费。
每笔 fill 有 source_trade_id。
```

---

### Task 9：Report builder

文件：

```text
src/backtest/orderfilled_only/report.py
```

输出：

```text
orders_total
orders_attempted
orders_filled
orders_partial
orders_unfilled_no_same_side_trade
orders_unfilled_limit_price
orders_unfilled_participation_cap
simulated_volume
eligible_historical_volume
simulated_volume / eligible_historical_volume
fill_rate
partial_fill_rate
avg_fill_delay
avg_price_buffer
capacity_utilization
PnL after fees
strict / conservative / optimistic sensitivity
participation curve
latency curve
horizon curve
```

---

## 13. 测试方案

### 13.1 Normalizer tests

#### maker buy

输入：

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

#### maker sell

输入：

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

#### 异常数据

```text
price <= 0 -> quarantine
price > 1 -> quarantine
size <= 0 -> quarantine
unknown asset_id -> quarantine
unsupported asset pair -> quarantine
```

---

### 13.2 Trade print builder tests

输入 maker fills：

```text
tx A log 1 BUY @ 0.51 size 100
tx A log 2 BUY @ 0.51 size 200
tx A log 3 BUY @ 0.52 size 300
```

期望：

```text
trade_print 1: BUY @ 0.51 size 300 source=[log1,log2]
trade_print 2: BUY @ 0.52 size 300 source=[log3]
```

不要生成：

```text
BUY @ 0.5167 size 600
```

---

### 13.3 Block bar tests

输入：

```text
block 100 trade 1 BUY  @ 0.51 size 100
block 100 trade 2 BUY  @ 0.52 size 200
block 100 trade 3 SELL @ 0.50 size 500
block 103 trade 4 BUY  @ 0.53 size 1000
```

期望 sparse：

```text
block 100:
open=0.51 high=0.52 low=0.50 close=0.50 volume=800 buy_volume=300 sell_volume=500 trade_count=3

block 103:
open=0.53 high=0.53 low=0.53 close=0.53 volume=1000 buy_volume=1000 sell_volume=0 trade_count=1
```

期望 dense：

```text
block 101:
mark=0.50 volume=0 trade_count=0 is_carry_forward=true price_age_blocks=1

block 102:
mark=0.50 volume=0 trade_count=0 is_carry_forward=true price_age_blocks=2
```

---

### 13.4 Taker tests

#### BUY taker direction

Trade tape：

```text
BUY  @ 0.52 size 1000
SELL @ 0.51 size 1000
```

Order：

```text
BUY limit 0.53 size 100 participation=2.5%
```

期望：

```text
只能用 BUY @ 0.52。
最多 fill 25。
不能用 SELL @ 0.51。
```

#### SELL taker direction

Order：

```text
SELL limit 0.50 size 100 participation=2.5%
```

期望：

```text
只能用 SELL @ 0.51。
最多 fill 25。
不能用 BUY @ 0.52。
```

#### Limit check

```text
BUY limit 0.52 不能用 BUY @ 0.53。
SELL limit 0.50 不能用 SELL @ 0.49。
```

#### No trade no fill

```text
arrival_ts 后 horizon 内无合格 trade -> 0 fill。
```

#### Capacity no double count

两个 BUY order 同时参与同一 trade：

```text
trade size=1000, participation=2.5%, total capacity=25
```

期望：

```text
两个订单合计 fill <= 25。
```

---

### 13.5 Maker tests

#### strict mode

```text
maker_mode=strict_no_fill
```

任何 maker order：

```text
0 fill
state=WORKING_BUT_NON_EXECUTABLE_IN_ORDERFILLED_ONLY
not included in PnL
```

#### BUY maker phantom queue

Order：

```text
BUY @ 0.45 size 100
phantom_queue=1000
```

Trades：

```text
SELL @ 0.45 size 300
SELL @ 0.45 size 500
SELL @ 0.45 size 400
```

期望：

```text
前两笔只消耗 queue：1000 -> 700 -> 200
第三笔先消耗 200，剩余 flow 才 fill order
```

#### Wrong side

```text
BUY maker 不被 BUY aggressor trade 推进。
SELL maker 不被 SELL aggressor trade 推进。
```

#### Wrong price

```text
BUY @ 0.45 不被 SELL @ 0.46 推进。
SELL @ 0.55 不被 BUY @ 0.54 推进。
```

---

### 13.6 Look-ahead tests

必须测试：

```text
1. fill_ts >= arrival_ts。
2. signal_ts 之前的 trade 不能用于 fill。
3. same block 后续 log 不能默认用于 fill。
4. dense mark 的 carry-forward price 不能用于 fill。
5. block high/low 不能用于 fill。
```

---

### 13.7 Property tests / invariants

长期不变量：

```text
fill.size > 0
order.filled_size <= order.original_size
allocated_trade_capacity <= trade.size * participation_rate
fill_ts >= arrival_ts
BUY fill_price <= limit_price
SELL fill_price >= limit_price
maker fill_price == order.limit_price
maker strict mode produces zero fills
no OrderFilled-derived trade evidence -> no fill
same input -> same output
any fill has source_trade_ids
raw_orderfilled is never consumed by execution engine directly
block_marks_dense is never consumed by execution engine directly
```

---

### 13.8 Real data validation

#### 数据质量报告

```text
raw_orderfilled_count
maker_fill_ticks_count
quarantine_count
trade_prints_count
quarantine_rate
price_out_of_range_count
unknown_asset_count
one_sided_volume_by_market
raw_sum_vs_one_sided_sum_ratio
```

#### Wallet replay

选真实钱包：

```text
1. 用 maker_fill_ticks 重建钱包真实成交。
2. 用 trade_prints_one_sided 检查对应 trade tape。
3. 用小 participation 模拟 copy order。
4. 对比方向、价格、size、fee。
```

#### Paper/live 校准

用小单校准：

```text
participation_rate
latency_ms
price_buffer
phantom_queue
execution_horizon
```

指标：

```text
predicted fill probability vs actual
predicted fill size vs actual
predicted fill price vs actual
false positive fills
false negative fills
```

---

### 13.9 Sensitivity / robustness

每次报告固定输出：

```text
participation_rate curve:
    0.5%, 1%, 2.5%, 5%, 10%

latency curve:
    0ms, 200ms, 1000ms, 5000ms

horizon curve:
    5s, 30s, 5m, 30m

mode comparison:
    strict_audit
    conservative_trade_tape
    optimistic_sensitivity
```

如果策略只有在：

```text
participation_rate >= 10%
latency = 0ms
optimistic maker
```

才赚钱，则报告标记：

```text
execution_sensitive = true
capacity_risk = high
```

---

## 14. 开发反模式清单

禁止实现：

```text
1. 直接用 raw_orderfilled sum volume。
2. 直接用 block close 成交。
3. 直接用 high/low 判断成交。
4. 无成交 block 用 forward-fill price 成交。
5. 不区分 maker_fill_ticks 和 trade_prints_one_sided。
6. 不做 one-sided volume，导致双算。
7. BUY 使用 SELL trade print 成交，或 SELL 使用 BUY trade print 成交。
8. 忽略 limit price。
9. 忽略 latency。
10. 让 fill_ts < arrival_ts。
11. 同一 trade capacity 被多个订单重复消耗。
12. Maker 默认直接成交。
13. 把 phantom maker 结果当唯一主结果。
14. 只输出 optimistic PnL。
15. 没有 source_trade_id 的 fill。
```

---

## 15. 最小实现阶段

### Phase 1：数据处理 MVP

实现：

```text
raw_orderfilled -> maker_fill_ticks
maker_fill_ticks -> trade_prints_one_sided
trade_prints_one_sided -> block_trade_bars_sparse
trade_prints_one_sided -> block_marks_dense
trade_prints_one_sided -> time_bars
```

验收：

```text
quarantine 可解释。
one-sided volume 可解释。
block sparse/dense bars 正确。
无成交 block 不进 sparse。
dense stale 标记正确。
```

---

### Phase 2：Taker execution MVP

实现：

```text
strict_audit taker
capacity ledger
latency
limit check
price buffer
audit log
```

验收：

```text
不看 OHLCV。
不看 dense marks。
只参与 future same-side trade prints。
容量不超 participation cap。
每笔 fill 可追溯。
```

---

### Phase 3：Maker sensitivity

实现：

```text
maker strict_no_fill
maker phantom_queue
maker sensitivity report
```

验收：

```text
默认主 PnL 不包含 maker phantom。
phantom 结果单独展示。
wrong side/wrong price 不推进。
```

---

### Phase 4：验证与报告

实现：

```text
unit tests
property tests
real data validation
wallet replay
sensitivity curves
report markdown/html
```

验收：

```text
报告不只给 PnL。
报告给 fill evidence、capacity、unfilled reasons、sensitivity。
```

---

## 16. 最终验收标准

这个系统写对的标志：

```text
1. `OrderFilled` 被处理成 raw_orderfilled、maker_fill_ticks、trade_prints_one_sided 三层。
2. 执行层只读 trade_prints_one_sided。
3. block OHLCV 存在，但不驱动成交。
4. 无交易 block 可以有 stale mark，但不能成交。
5. 同一个 block 内多笔成交保留 tx/log 顺序。
6. BUY/SELL 方向映射正确。
7. 模拟成交量不超过 participation cap。
8. 每笔 fill 都有 source_trade_id / tx_hash。
9. Taker 不使用错误方向成交。
10. Maker 默认严格不成交；phantom queue 只做 sensitivity。
11. 没有 OrderFilled-derived trade evidence 就没有 fill。
12. 报告明确声明：不是 L2 depth / queue-accurate simulation。
```

---

## 17. 推荐文件结构

```text
src/backtest/orderfilled_only/
    __init__.py
    types.py
    normalizer.py
    trade_print_builder.py
    bars.py
    marks.py
    capacity.py
    taker.py
    maker.py
    engine.py
    audit.py
    report.py
    validation.py

tests/orderfilled_only/
    test_normalizer.py
    test_trade_print_builder.py
    test_bars.py
    test_capacity.py
    test_taker.py
    test_maker.py
    test_engine_lookahead.py
    test_invariants.py
    test_real_data_smoke.py
```

---

## 18. 给 Codex 的最终指令

Codex 实现时优先顺序：

```text
1. 先写 tests，不要先写 engine。
2. 先写 OrderFilled normalizer。
3. 再写 one-sided trade print builder。
4. 再写 block/time bars。
5. 再写 capacity ledger。
6. 再写 strict taker execution。
7. 最后写 maker phantom sensitivity。
8. 每个 fill 必须 audit-friendly。
```

默认主结果：

```text
strict_audit 或 conservative_trade_tape
```

不要默认展示：

```text
optimistic_sensitivity
```

最终报告标题建议：

```text
OrderFilled-only Trade-Tape Backtest
Execution-evidence constrained; not L2 depth / queue accurate
```
