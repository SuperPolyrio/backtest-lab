# OrderFilled V2 回测加速指导文档：从 `O(orders * trades)` 到 indexed / streaming matcher

> 目标读者：Codex / 开发者  
> 目标：在不改变 OrderFilled-only 成交语义的前提下，把当前可运行但朴素的 replay matcher 从小样本 smoke 工具升级成可跑真实大规模数据的回测执行引擎。  
> 当前问题：规则验证已通过，但性能验收未通过。瓶颈在 matcher hot path：每个 order 从头扫描全部 loaded trades，复杂度接近 `O(num_orders * num_loaded_trades)`。

---

## 0. 结论

当前需要改的是 **数据切片 + 匹配器 hot path**，不是推倒整个回测框架。

保留：

```text
raw_orderfilled -> maker_fill_ticks -> trade_prints_one_sided 数据主线
OrderFilled-only execution rules
capacity ledger 规则
strict / conservative / optimistic profiles
report / persistence / tests
slow reference implementation
```

新增：

```text
RequiredTradeWindowBuilder
TradeSliceLoader
TradeGroupIndex
IndexedTakerMatcher
ArrayCapacityLedger
BenchmarkSuite
Reference-vs-Indexed correctness comparator
```

第一阶段不要先换 Rust，不要先换高频回测框架，也不要把问题甩给数据库。先把算法从：

```text
for each order:
    scan all loaded trades
```

改成：

```text
for each order:
    use (market_id, asset_id, aggressor_side) group
    binary search arrival/deadline
    scan only candidate trades in that window
```

目标复杂度从：

```text
O(num_orders * num_loaded_trades)
```

降到：

```text
O(num_loaded_trades sorting/indexing)
+ O(num_orders * log(trades_per_group))
+ O(candidate_trades_scanned)
```

---

## 1. 不允许改变的成交语义

这次任务只做加速，不改成交规则。

### 1.1 执行层主数据仍然是 `trade_prints_one_sided`

执行层只能使用：

```text
trade_prints_one_sided
```

不能使用：

```text
raw_orderfilled
maker_fill_ticks
block_trade_bars_sparse
time_bars
block_marks_dense
OHLCV close/high/low
```

原因：`trade_prints_one_sided` 是去重/one-sided 后的真实成交 tape，才适合作为 capacity ledger 和 OrderFilled-only execution 的输入。

---

### 1.2 Taker 规则不能变

BUY taker：

```text
只能用 aggressor_side = BUY 的 future trade prints
trade.price <= order.limit_price
trade.event_key >= order.arrival_key
trade.event_key <= order.deadline_key
每条 trade 最多消耗 participation_rate * trade.size
```

SELL taker：

```text
只能用 aggressor_side = SELL 的 future trade prints
trade.price >= order.limit_price
trade.event_key >= order.arrival_key
trade.event_key <= order.deadline_key
每条 trade 最多消耗 participation_rate * trade.size
```

无合格 trade print：

```text
不成交
```

不能用：

```text
last price
block close
forward-filled mark
OHLCV high/low
```

---

### 1.3 Capacity ledger 不能变

每条 historical trade print 的总可分配容量：

```text
trade_capacity = trade.size * participation_rate
```

在同一个 run/profile 内，多个模拟订单共享这条 trade 的容量：

```text
sum(consumed_by_all_orders[trade_id]) <= trade_capacity
```

不能给每个 order 单独一份 capacity 副本。

---

### 1.4 输出必须和 reference engine 对齐

保留当前慢版本作为 oracle：

```text
SlowReferenceMatcher
```

新增快版本：

```text
IndexedTakerMatcher
```

在 small / medium fixture 上必须满足：

```text
fills 完全一致
capacity ledger 完全一致
orders final state 完全一致
metrics 完全一致
```

如果不一致，优先相信 reference engine，除非确认 reference 有 bug。

---

## 2. 当前慢在哪里

当前伪逻辑类似：

```python
def replay_v2_taker_orders(orders, trades):
    ordered_trades = sorted(trades)
    for order in orders:
        replay_v2_taker_order(order, ordered_trades)


def replay_v2_taker_order(order, ordered_trades):
    for trade in ordered_trades:
        if trade.market_id != order.market_id:
            continue
        if trade.asset_id != order.asset_id:
            continue
        if trade.aggressor_side != order.side:
            continue
        if trade.event_key < order.arrival_key:
            continue
        if trade.event_key > order.deadline_key:
            continue
        if not limit_ok(order, trade):
            continue
        # consume capacity
```

问题：每个 order 都扫描 `ordered_trades`，即使 99.99% 的 trades 属于别的 market、别的 asset、别的 side、别的时间窗口。

当前实测：

```text
1,000 trades / 100 orders     -> 0.0444s
10,000 trades / 1,000 orders  -> 4.0062s
50,000 trades / 2,000 orders  -> 43.607s
```

这说明 hot path 近似 `O(orders * loaded_trades)`。

---

## 3. 目标架构

新增数据加速架构：

```text
Strategy orders
    ↓
RequiredTradeWindowBuilder
    ↓
Merged windows by (market_id, asset_id, aggressor_side)
    ↓
TradeSliceLoader
    ↓
TradeGroupIndex
    ↓
IndexedTakerMatcher
    ↓
ArrayCapacityLedger
    ↓
Execution fills / order states / capacity audit
    ↓
Reference-vs-Indexed comparator + benchmark report
```

---

## 4. 核心数据结构

### 4.1 EventKey

必须把事件顺序抽象成稳定可比较的 key。

推荐：

```python
@dataclass(frozen=True, order=True)
class EventKey:
    block_number: int
    tx_index: int
    log_index: int
```

如果没有 `tx_index`：

```python
@dataclass(frozen=True, order=True)
class EventKey:
    block_number: int
    tx_sort_key: int      # stable hash/rank of tx_hash inside block
    log_index: int
```

如果使用 timestamp：

```python
@dataclass(frozen=True, order=True)
class EventKey:
    ts_ns: int
    block_number: int
    tx_index: int
    log_index: int
```

验收要求：

```text
同一批输入每次生成的 EventKey 完全一致。
EventKey 能严格排序同一 block 内的 trade prints。
order.arrival_key 和 order.deadline_key 必须使用同一套 key 空间。
```

---

### 4.2 TradeRecord

第一版可以沿用现有 dataclass，但 indexed hot path 建议转换成紧凑字段。

```python
@dataclass(frozen=True)
class TradeRecord:
    trade_id: str
    market_id: str
    asset_id: str
    aggressor_side: Literal["BUY", "SELL"]
    event_key: EventKey
    price_ticks: int
    size_atoms: int
    tx_hash: str
    log_indexes: tuple[int, ...]
```

注意：

```text
price 和 size 在 hot path 尽量使用 integer。
外部输入/输出可以继续 Decimal。
```

推荐转换：

```text
price_ticks = int(price / tick_size)
size_atoms = int(size * SIZE_SCALE)
```

如果暂时 tick size 不统一，可以先用：

```text
price_ppm = int(price * 1_000_000)
size_atoms = int(size * 1_000_000)
```

---

### 4.3 OrderRecord

```python
@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    market_id: str
    asset_id: str
    side: Literal["BUY", "SELL"]
    arrival_key: EventKey
    deadline_key: EventKey
    limit_price_ticks: int
    size_atoms: int
    priority_key: tuple
    config_profile: str
```

`priority_key` 必须稳定：

```text
arrival_key
strategy_sequence
client_order_id
```

多个模拟订单争同一条 trade capacity 时，按 `priority_key` 分配。

---

### 4.4 TradeGroupKey

```python
TradeGroupKey = tuple[str, str, str]
# (market_id, asset_id, aggressor_side)
```

对于 taker：

```text
BUY order 只查 key=(market_id, asset_id, "BUY")
SELL order 只查 key=(market_id, asset_id, "SELL")
```

---

### 4.5 TradeGroupIndex

```python
@dataclass
class TradeGroupIndex:
    key: TradeGroupKey
    event_keys: list[EventKey]
    trade_ids: list[str]
    price_ticks: list[int]
    size_atoms: list[int]
    capacity_remaining_atoms: list[int]
    tx_hashes: list[str]
    log_indexes: list[tuple[int, ...]]
```

要求：

```text
event_keys 必须升序。
所有 list 长度一致。
capacity_remaining_atoms[i] 初始化为 size_atoms[i] * participation_rate。
```

---

## 5. RequiredTradeWindowBuilder

### 5.1 目标

不要全量加载 9 亿 trade prints。先根据订单生成真正需要的 trade windows。

每个 taker order 生成：

```text
market_id
asset_id
aggressor_side = order.side
start_key = arrival_key
end_key = deadline_key
```

---

### 5.2 数据结构

```python
@dataclass(frozen=True)
class RequiredTradeWindow:
    market_id: str
    asset_id: str
    aggressor_side: Literal["BUY", "SELL"]
    start_key: EventKey
    end_key: EventKey
```

---

### 5.3 窗口合并

同一个 `(market_id, asset_id, aggressor_side)` 下，合并重叠窗口：

```text
[100, 200] + [180, 250] -> [100, 250]
[100, 200] + [300, 400] -> 保持两段
```

可以支持 `merge_gap_blocks`：

```text
如果两个窗口间隔很小，例如 <= 5 blocks，也合并，减少 DB query 次数。
```

伪代码：

```python
def merge_windows(windows, merge_gap_blocks=0):
    by_key = defaultdict(list)
    for w in windows:
        by_key[(w.market_id, w.asset_id, w.aggressor_side)].append(w)

    merged = []
    for key, ws in by_key.items():
        ws.sort(key=lambda w: w.start_key)
        cur = ws[0]
        for nxt in ws[1:]:
            if nxt.start_key <= extend_key(cur.end_key, merge_gap_blocks):
                cur = RequiredTradeWindow(
                    market_id=cur.market_id,
                    asset_id=cur.asset_id,
                    aggressor_side=cur.aggressor_side,
                    start_key=cur.start_key,
                    end_key=max(cur.end_key, nxt.end_key),
                )
            else:
                merged.append(cur)
                cur = nxt
        merged.append(cur)
    return merged
```

---

## 6. TradeSliceLoader

### 6.1 目标

批量加载每个 window 的 trades。不要 per-order SQL。

禁止：

```python
for order in orders:
    sql_query(order.market_id, order.asset_id, order.arrival, order.deadline)
```

允许：

```python
windows = build_required_windows(orders)
merged_windows = merge_windows(windows)
for window in merged_windows:
    load all trades for this window
```

---

### 6.2 Postgres loader

建议 SQL：

```sql
SELECT
    trade_id,
    market_id,
    asset_id,
    aggressor_side,
    block_number,
    tx_index,
    log_index,
    price,
    size,
    tx_hash,
    source_orderfilled_ids
FROM trade_prints_one_sided
WHERE market_id = %(market_id)s
  AND asset_id = %(asset_id)s
  AND aggressor_side = %(aggressor_side)s
  AND block_number BETWEEN %(start_block)s AND %(end_block)s
ORDER BY block_number, tx_index, log_index;
```

如果 order/window 使用 timestamp，也要限制 block range，避免纯 timestamp 扫大表。

---

### 6.3 Postgres 索引建议

在大表上先检查已有索引，不要盲目重复建。

建议索引：

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tpos_exec_lookup
ON trade_prints_one_sided (
    market_id,
    asset_id,
    aggressor_side,
    block_number,
    tx_index,
    log_index
);
```

如果表是 append-only 且按 block 排列，可以考虑：

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS brin_tpos_block_number
ON trade_prints_one_sided USING BRIN (block_number);
```

如果表非常大，优先考虑分区：

```text
partition by block range / date
```

注意：索引解决的是 **读哪些 trades**，不解决 **如何撮合订单与 capacity**。

---

### 6.4 Parquet / DuckDB loader

如果 `trade_prints_one_sided` 有 Parquet 物化版，建议支持：

```text
DuckDB / Polars scan parquet
按 market_id / asset_id / aggressor_side / block range predicate pushdown
```

DuckDB 伪 SQL：

```sql
SELECT *
FROM read_parquet('/data2/orderfilled/trade_prints_one_sided/**/*.parquet')
WHERE market_id = ?
  AND asset_id = ?
  AND aggressor_side = ?
  AND block_number BETWEEN ? AND ?
ORDER BY block_number, tx_index, log_index;
```

第一版可以先用 Postgres；后续如果 IO 成为瓶颈，再加 Parquet/DuckDB loader。

---

### 6.5 Loader 统计指标

每次 load 输出：

```text
windows_count
merged_windows_count
trade_rows_loaded
trade_groups_count
db_query_count
db_load_sec
db_rows_per_sec
```

---

## 7. IndexedTakerMatcher V1

### 7.1 构建索引

```python
def build_trade_group_index(trades, participation_rate) -> dict[TradeGroupKey, TradeGroupIndex]:
    grouped = defaultdict(list)
    for t in trades:
        grouped[(t.market_id, t.asset_id, t.aggressor_side)].append(t)

    indexes = {}
    for key, rows in grouped.items():
        rows.sort(key=lambda t: t.event_key)
        indexes[key] = TradeGroupIndex(
            key=key,
            event_keys=[t.event_key for t in rows],
            trade_ids=[t.trade_id for t in rows],
            price_ticks=[t.price_ticks for t in rows],
            size_atoms=[t.size_atoms for t in rows],
            capacity_remaining_atoms=[int(t.size_atoms * participation_rate) for t in rows],
            tx_hashes=[t.tx_hash for t in rows],
            log_indexes=[t.log_indexes for t in rows],
        )
    return indexes
```

如果一条 trade 的 capacity 计算需要 profile 相关参数，不同 profile 必须使用不同 index 或不同 capacity array。

---

### 7.2 匹配一个 order

```python
from bisect import bisect_left, bisect_right


def match_order_indexed(order: OrderRecord, index: dict[TradeGroupKey, TradeGroupIndex], config):
    key = (order.market_id, order.asset_id, order.side)
    group = index.get(key)
    if group is None:
        return [], order.size_atoms, "no_trade_group"

    i = bisect_left(group.event_keys, order.arrival_key)
    j_end = bisect_right(group.event_keys, order.deadline_key)

    remaining = order.size_atoms
    fills = []
    candidate_scanned = 0

    for j in range(i, j_end):
        candidate_scanned += 1

        cap = group.capacity_remaining_atoms[j]
        if cap <= 0:
            continue

        trade_price = group.price_ticks[j]

        if order.side == "BUY":
            if trade_price > order.limit_price_ticks:
                continue
            exec_price = trade_price + config.price_buffer_ticks
            if exec_price > order.limit_price_ticks:
                continue
        else:
            if trade_price < order.limit_price_ticks:
                continue
            exec_price = trade_price - config.price_buffer_ticks
            if exec_price < order.limit_price_ticks:
                continue

        qty = min(remaining, cap)
        if qty <= 0:
            continue

        group.capacity_remaining_atoms[j] -= qty
        remaining -= qty

        fills.append({
            "order_id": order.order_id,
            "trade_id": group.trade_ids[j],
            "exec_price_ticks": exec_price,
            "size_atoms": qty,
            "source_tx_hash": group.tx_hashes[j],
            "source_log_indexes": group.log_indexes[j],
            "candidate_index": j,
        })

        if remaining <= 0:
            break

    reason = "filled" if remaining == 0 else "insufficient_post_arrival_same_side_capacity"
    return fills, remaining, reason, candidate_scanned
```

---

### 7.3 匹配所有 orders

必须固定 order 顺序：

```python
def match_orders_indexed(orders, trades, config):
    orders = sorted(orders, key=lambda o: o.priority_key)
    index = build_trade_group_index(trades, config.participation_rate)

    all_fills = []
    order_results = []
    total_candidates_scanned = 0

    for order in orders:
        fills, remaining, reason, scanned = match_order_indexed(order, index, config)
        all_fills.extend(fills)
        order_results.append(build_order_result(order, fills, remaining, reason))
        total_candidates_scanned += scanned

    return ReplayResult(
        fills=all_fills,
        orders=order_results,
        metrics={
            "candidate_trades_scanned": total_candidates_scanned,
            "trade_groups": len(index),
        },
    )
```

---

## 8. ArrayCapacityLedger

### 8.1 为什么需要数组

如果每次消费 capacity 都查 dict：

```python
capacity_remaining_by_trade_id[trade_id]
```

在大规模下会慢。索引内可以用数组：

```text
capacity_remaining_atoms[j]
```

审计时再把 `j` 映射回 `trade_id`。

---

### 8.2 仍然要输出审计 ledger

虽然 hot path 用数组，但结果里必须输出：

```text
trade_id
capacity_initial
capacity_consumed
capacity_remaining
consuming_order_ids
```

可以在 replay 结束后从 fills 聚合生成：

```python
for fill in fills:
    ledger[fill.trade_id].consumed += fill.size_atoms
    ledger[fill.trade_id].order_ids.append(fill.order_id)
```

验收：

```text
capacity_consumed <= trade.size * participation_rate
```

---

## 9. 进一步优化：StreamingTakerMatcher V2

Indexed V1 已经能解决最主要问题。如果 V1 仍然慢，通常是因为：

```text
大量 orders 的 time windows 高度重叠
每个 order 仍然反复扫描相同 candidate trades
```

这时实现 V2：trade-centric streaming matcher。

### 9.1 Streaming 思路

对每个 key：

```text
(market_id, asset_id, aggressor_side)
```

合并事件：

```text
order arrival
trade print
order deadline
```

按时间推进：

```text
arrival event -> order 进入 active pool
trade event   -> trade capacity 分配给 active eligible orders
deadline event -> order 过期
```

复杂度接近：

```text
O((orders + trades) log active_orders + fills)
```

---

### 9.2 V2 暂不作为第一目标

先完成 V1，因为 V2 需要处理：

```text
active orders 的价格过滤
同一 trade capacity 分配优先级
deadline eviction
partial fills
price buffer
maker phantom queue later
```

只有当 V1 benchmark 显示候选扫描仍是瓶颈，再做 V2。

---

## 10. 数据库不是 matcher

数据库负责：

```text
存储大表
根据 required windows 批量加载相关 trades
保存 run / orders / fills / metrics / audit
```

数据库不负责：

```text
每个 order 执行一条 SQL
在 SQL 里维护 capacity ledger
在 SQL 里分配同一 trade 的容量给多个订单
```

禁止模式：

```python
for order in orders:
    SELECT trades WHERE order window
    match order
```

推荐模式：

```text
orders -> required windows -> bulk load -> in-memory indexed matching -> persist results
```

---

## 11. Rust 不是第一步

不要先把当前朴素扫描改成 Rust。

原因：

```text
O(orders * trades) 用 Rust 还是 O(orders * trades)。
语言优化不能替代算法优化。
```

只有满足以下条件再考虑 Rust：

```text
1. Indexed/Streaming matcher 已完成。
2. 没有 per-order full scan。
3. 没有 per-order SQL。
4. profiler 显示 70%+ 时间在 CPU matching hot loop。
5. matching 规则已经稳定。
```

如果未来需要 Rust，只 port：

```text
matching kernel
TradeGroup arrays
capacity_remaining arrays
```

不要重写整个框架。

---

## 12. Codex 开发任务拆分

### Task 1：保留 reference engine

文件：

```text
quant/backtest/orderfilled_v2_replay.py
```

要求：

```text
当前 replay_v2_taker_orders / replay_v2_taker_order 保留。
重命名或包装为 reference 版本。
不要为了性能删除 reference。
```

建议：

```python
replay_v2_taker_orders_reference(...)
replay_v2_taker_order_reference(...)
```

验收：

```text
现有 33 PASS / 0 FAIL 继续通过。
smoke 继续通过。
```

---

### Task 2：新增 event key / compact record 类型

新增文件：

```text
quant/backtest/orderfilled_v2_index_types.py
```

实现：

```text
EventKey
TradeGroupKey
TradeRecordCompact
OrderRecordCompact
RequiredTradeWindow
TradeGroupIndex
```

验收：

```text
EventKey 可排序。
同 block 多 tx/log 顺序稳定。
OrderRecordCompact 能从现有 order object 转换。
TradeRecordCompact 能从 trade_prints_one_sided rows 转换。
```

---

### Task 3：RequiredTradeWindowBuilder

新增文件：

```text
quant/backtest/orderfilled_v2_windows.py
```

实现：

```text
build_required_trade_windows(orders)
merge_required_trade_windows(windows, merge_gap_blocks)
```

验收测试：

```text
BUY order -> aggressor_side BUY window
SELL order -> aggressor_side SELL window
overlapping windows merge
non-overlapping windows 不 merge
merge_gap_blocks 生效
```

---

### Task 4：TradeSliceLoader

新增文件：

```text
quant/backtest/orderfilled_v2_trade_loader.py
```

实现接口：

```python
class TradeSliceLoader(Protocol):
    def load_trades(self, windows: list[RequiredTradeWindow]) -> list[TradeRecordCompact]:
        ...
```

实现两个版本：

```text
InMemoryTradeSliceLoader：用于 tests / synthetic benchmark
PostgresTradeSliceLoader：用于真实数据
```

可选第三个：

```text
DuckDBParquetTradeSliceLoader：用于 parquet 大切片
```

验收：

```text
loader 不做 per-order query。
loader 输出 rows 已按 group 或整体可排序。
loader metrics 包含 windows_count、merged_windows_count、query_count、rows_loaded、load_sec。
```

---

### Task 5：TradeGroupIndexBuilder

新增文件：

```text
quant/backtest/orderfilled_v2_indexed_matcher.py
```

实现：

```text
build_trade_group_index(trades, participation_rate)
```

验收：

```text
按 (market_id, asset_id, aggressor_side) 分组。
组内 event_keys 升序。
capacity_remaining = size * participation_rate。
```

---

### Task 6：IndexedTakerMatcher

同文件：

```text
quant/backtest/orderfilled_v2_indexed_matcher.py
```

实现：

```text
match_order_indexed
match_orders_indexed
```

要求：

```text
使用 bisect 定位 arrival/deadline。
只扫描 candidate window 内 trades。
使用共享 capacity_remaining 数组。
输出 fills、order results、capacity ledger、metrics。
```

验收：

```text
小 fixture 与 reference 完全一致。
capacity 不超用。
wrong_side = 0。
limit_violations = 0。
lookahead = 0。
source trade mismatch = 0。
```

---

### Task 7：Reference-vs-Indexed comparator

新增文件：

```text
quant/backtest/orderfilled_v2_compare.py
```

实现：

```text
compare_replay_results(reference, indexed)
```

比较：

```text
order_id
filled_size
unfilled_size
fill_count
fill prices
source trade ids
capacity consumed by trade_id
final reason
```

输出：

```text
diff_count
first_n_diffs
reference_hash
indexed_hash
```

验收：

```text
synthetic small diff_count = 0
synthetic medium diff_count = 0
```

---

### Task 8：Benchmark suite

新增文件：

```text
scripts/benchmark_orderfilled_v2_indexed_replay.py
```

支持：

```bash
python scripts/benchmark_orderfilled_v2_indexed_replay.py \
  --dataset synthetic \
  --trades 50000 \
  --orders 2000 \
  --seed 42
```

支持真实数据：

```bash
python scripts/benchmark_orderfilled_v2_indexed_replay.py \
  --dataset historical \
  --market-count 50 \
  --start-block 70000000 \
  --end-block 70100000
```

输出：

```text
runtime_total_sec
load_sec
index_build_sec
matching_sec
persist_sec
orders_count
trades_loaded
trade_groups_count
candidate_trades_scanned
candidate_rows_per_order_p50
candidate_rows_per_order_p95
candidate_rows_per_order_p99
fills_count
capacity_updates_count
orders_per_sec
trades_loaded_per_sec
peak_memory_mb
reference_runtime_sec, if enabled
speedup_vs_reference
```

写入：

```text
runtime_outputs/orderfilled_v2_benchmarks/<timestamp>_indexed_benchmark.md
runtime_outputs/orderfilled_v2_benchmarks/<timestamp>_indexed_benchmark.json
```

---

### Task 9：性能回归检查

新增文件：

```text
scripts/check_orderfilled_v2_perf_regression.py
```

输入：

```text
baseline.json
current.json
max_slowdown_pct
```

检查：

```text
matching_sec slowdown <= threshold
orders_per_sec 不低于 threshold
candidate_rows_per_order_p95 不异常升高
```

示例：

```bash
python scripts/check_orderfilled_v2_perf_regression.py \
  --baseline runtime_outputs/orderfilled_v2_benchmarks/baseline.json \
  --current runtime_outputs/orderfilled_v2_benchmarks/current.json \
  --max-slowdown-pct 20
```

---

## 13. 测试计划

### 13.1 Correctness tests

新增：

```text
quant/backtest/tests/test_orderfilled_v2_indexed_matcher.py
```

必须覆盖：

```text
1. BUY direction：BUY order 只用 BUY trade。
2. SELL direction：SELL order 只用 SELL trade。
3. BUY limit：不能用 price > limit 的 BUY trade。
4. SELL limit：不能用 price < limit 的 SELL trade。
5. latency/lookahead：不能用 arrival 前 trade。
6. deadline：不能用 deadline 后 trade。
7. capacity shared：两个订单不能重复用同一 trade capacity。
8. no group：没有 group 时 no fill。
9. empty window：window 内没有 candidate 时 no fill。
10. deterministic priority：同输入多次输出 hash 一致。
```

---

### 13.2 Reference parity tests

新增测试：

```text
test_indexed_matches_reference_small
test_indexed_matches_reference_medium_randomized
test_indexed_matches_reference_capacity_contention
test_indexed_matches_reference_same_block_ordering
```

要求：

```text
diff_count = 0
```

---

### 13.3 Metamorphic tests

新增测试：

```text
participation_rate 增大，filled_size 不应下降。
BUY limit 放宽，filled_size 不应下降。
SELL limit 放宽，filled_size 不应下降。
horizon 变长，filled_size 不应下降。
price_buffer 变差，filled_size 不应上升。
删除 trade prints 后，filled_size 不应上升。
```

---

### 13.4 Performance tests

不建议放进默认 `pytest`，可以单独 marker：

```bash
pytest -m performance
```

基准：

```text
1k trades / 100 orders
10k trades / 1k orders
50k trades / 2k orders
100k trades / 5k orders
1M trades / 10k orders
```

验收建议：

```text
50k trades / 2k orders 必须明显快于 reference，建议至少 20x。
1M trades / 10k orders 不 OOM，且 runtime 随规模接近线性。
```

不要把绝对秒数写死为唯一 gate，因为不同机器差异大。以相对 reference speedup 和复杂度指标为主。

---

## 14. 指标解释

### 14.1 candidate_trades_scanned

这是核心指标。

当前朴素算法等价于：

```text
candidate_trades_scanned ≈ orders * loaded_trades
```

Indexed matcher 应该变成：

```text
candidate_trades_scanned = 所有 order 的真实 group/window 内 trades 数量之和
```

报告必须输出：

```text
candidate_trades_scanned
orders * loaded_trades
scan_reduction_ratio = candidate_trades_scanned / (orders * loaded_trades)
```

如果 `scan_reduction_ratio` 仍然接近 1，说明没有真正加速。

---

### 14.2 candidate_rows_per_order_p95

如果 p95 很高，说明：

```text
execution_horizon 太长
window merge 太粗
market/asset 活跃度太高
需要 StreamingMatcher
```

---

### 14.3 db_time / total_time

如果：

```text
db_load_sec / runtime_total_sec > 70%
```

说明瓶颈在 IO/数据库，不在 matcher。下一步才考虑：

```text
Parquet/DuckDB loader
更细分区
更好索引
cache run slice
```

---

### 14.4 matching_time / total_time

如果：

```text
matching_sec / runtime_total_sec > 70%
```

且已经 indexed，那么考虑：

```text
integer hot path
array capacity ledger
StreamingMatcher
Numba/Rust kernel
```

---

## 15. 数据缓存策略

### 15.1 RunSliceCache

对于同一个 run 或同一批 orders，required windows 不变，可以缓存 loaded trades：

```text
runtime_outputs/orderfilled_v2_cache/run_<hash>/trades.parquet
runtime_outputs/orderfilled_v2_cache/run_<hash>/orders.parquet
runtime_outputs/orderfilled_v2_cache/run_<hash>/manifest.json
```

manifest：

```json
{
  "run_slice_hash": "...",
  "orders_hash": "...",
  "windows_hash": "...",
  "trade_rows": 123456,
  "created_at": "...",
  "source": "postgres",
  "query_count": 42
}
```

用于：

```text
重复 benchmark
reference-vs-indexed 对比
debug first diff
```

---

### 15.2 Cache 失效规则

缓存必须包含：

```text
trade_prints_table_version
query_config_hash
participation_rate/profile
orders_hash
```

如果任何一项变化，cache invalid。

---

## 16. 反模式清单

不要做：

```text
1. 为了快，改成交规则。
2. 删除 slow reference engine。
3. 每个 order 查一次数据库。
4. 每个 order 扫全部 loaded trades。
5. 每个 order 拿独立 capacity，不共享 trade capacity。
6. 用 OHLCV / close / high / forward-fill mark 做 execution。
7. 把 maker_fill_ticks 当 market one-sided volume 直接用。
8. 用 float 在 hot path 做价格/数量比较。
9. 不固定订单 capacity allocation priority。
10. 只跑 smoke，不跑 reference parity benchmark。
11. 一开始就重写 Rust。
12. 一开始就接入完整高频框架替换当前系统。
```

---

## 17. 最终验收标准

### 17.1 正确性

```text
现有规则测试全部通过。
indexed matcher 与 reference matcher 在 small/medium/randomized fixture 上 diff_count=0。
wrong_side = 0。
limit_violations = 0。
lookahead = 0。
capacity_overuse = 0。
source trade mismatch = 0。
同输入重复运行 hash 一致。
```

---

### 17.2 性能

```text
50k trades / 2k orders 明显快于 reference，建议 speedup >= 20x。
100k trades / 5k orders 能稳定跑完。
1M trades / 10k orders 不 OOM。
matching_sec 随 candidate_trades_scanned 接近线性。
不再随 orders * loaded_trades 二次爆炸。
```

---

### 17.3 数据加载

```text
不做 per-order SQL。
required windows 可合并。
loader 有 query_count / rows_loaded / load_sec 指标。
索引查询有 EXPLAIN ANALYZE 样本。
```

---

### 17.4 报告

benchmark 报告必须展示：

```text
reference_runtime_sec
indexed_runtime_sec
speedup_vs_reference
loaded_trades
orders
trade_groups
candidate_trades_scanned
scan_reduction_ratio
candidate_rows_per_order_p50/p95/p99
data_load_sec
index_build_sec
matching_sec
persist_sec
peak_memory_mb
```

---

## 18. 推荐实施顺序

```text
1. 给现有 matcher 改名为 reference。
2. 新增 EventKey / compact records。
3. 新增 RequiredTradeWindowBuilder。
4. 新增 InMemoryTradeSliceLoader。
5. 新增 TradeGroupIndexBuilder。
6. 新增 IndexedTakerMatcher。
7. 用 synthetic small 对齐 reference。
8. 用 synthetic medium 对齐 reference。
9. 加 PostgresTradeSliceLoader。
10. 跑 50k/2k benchmark，确认 >20x speedup。
11. 跑 1M/10k benchmark，确认不 OOM。
12. 加 benchmark markdown/json 报告。
13. 加性能回归脚本。
14. 如果 V1 仍慢，再设计 StreamingMatcher V2。
15. 如果 Streaming 后 CPU hot loop 仍是瓶颈，再考虑 Rust kernel。
```

---

## 19. 给 Codex 的最终指令

请按以下原则实现：

```text
只优化数据访问和 matcher hot path。
不要改变成交规则。
不要改变 capacity 语义。
不要让 OHLCV 进入 execution。
不要删除 reference engine。
所有 indexed 输出必须能和 reference 输出逐笔对齐。
所有 benchmark 必须输出 scan_reduction_ratio 和 speedup。
```

核心目标不是“让某个 smoke 更快”，而是让系统从：

```text
O(orders * loaded_trades)
```

升级为：

```text
O(loaded_trades + orders * log(group_size) + candidate_trades_scanned)
```

做到这一点以后，才继续讨论数据库分区、DuckDB/Parquet、Numba/Rust、StreamingMatcher。
