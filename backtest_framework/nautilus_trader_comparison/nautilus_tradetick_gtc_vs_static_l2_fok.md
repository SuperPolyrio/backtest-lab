# Nautilus TradeTick GTC 与静态 L2 FOK 对照

日期：2026-08-28

## 1. 结论

本次对照虽然都使用 NautilusTrader，但实际测试的是两套不同成交模型：

```text
TradeTick + GTC
    等待未来历史成交 tick 推动订单撮合。

Arrival-time static L2 + FOK
    使用订单到达前最后一份 L2，立即扫描限价内可见 ask。
```

`10/12 -> 12/12` 的直接原因是：静态 L2 路径允许模拟 taker 主动吃掉 ask，
不要求历史上随后出现 TradeTick。这个机制在 L2 新鲜时是合理的，因为模拟订单本来
就可能创造一笔反事实新成交。

原始静态脚本本身没有经过 PML2 状态机，所以当时只能把 `12/12` 写成上界。现在已用
同一冻结 cohort 重新运行完整 PML2 V2 validator：`strict_fok`、`realistic_fak`、
`wait30_fak`、`wait120_fak` 和 `optimistic_fak` 都得到 `12/12、120 股` observed fill。
事件流中没有 gap、clear、要求重同步或市场关闭事件，因此盘口按事件驱动规则持续有效。

2026-08-29 后，PML2 的正式规则已改为事件驱动：只要完整盘口已经同步，之后没有
gap、clear/reset、要求新 snapshot 的 lifecycle、未验证 handover、暂停或关闭事件，
盘口不会仅因经过若干秒而自动失效。真实 10-market/20-order 差分结果见
[`pml2_event_driven_real_markets.json`](./pml2_event_driven_real_markets.json)。

## 2. 两套实验的合同

| 项目 | TradeTick + GTC | Arrival-time static L2 + FOK |
| --- | --- | --- |
| 输入 | 历史逐笔成交 | 到达时刻之前最后一份 L2 |
| bid/ask/depth | 没有 | 有 |
| 推动成交的事件 | 未来 TradeTick | 到达时立即扫描 ask |
| 订单寿命 | GTC，但实验只提供 30 秒 tape | FOK，立即全成或失败 |
| 没有未来成交 | 通常不成交 | 盘口有足够 ask 即成交 |
| 容量来源 | TradeTick 合成的 L1 流动性 | L2 各价格档显示数量 |
| freshness | 不使用 L2 freshness | 此基线未检查 freshness |
| queue | 不准确 | Taker 扫盘不需要 Maker queue |
| 时间演进 | 有事件循环 | 静态快照，只计算一次 |

TradeTick 入口在
[`nautilus_real_trade_tape_comparison.py`](../../runtime_outputs/framework_comparison/nautilus_real_trade_tape_comparison.py#L83)。
静态 L2 扫盘在
[`offline_book_differential_worker.py`](../../scripts/offline_book_differential_worker.py#L112)，
真实 12 笔 L2 结果保存在
[`nautilus_results.json`](../../runtime_outputs/real_execution_model_comparison/nautilus_results.json)。

## 3. TradeTick + GTC 的实际含义

流程：

```text
signal trade
    -> 提交 BUY limit
    -> 1 秒模拟延迟
    -> 等待下一条 TradeTick
    -> 最多回放 signal 后 30 秒的 tape
```

虽然订单 TIF 是 `GTC`，但数据只提供 30 秒，所以本实验的真实含义是“在 30 秒成交带
内持续有效”，不是无限期等待。

Nautilus 在只有 TradeTick、没有历史 L2 时，会从成交 tick 建立合成市场状态。该状态
不是 Polymarket 当时真实记录的 L2。受控诊断中曾出现：

```text
历史 tick：1.11338 @ 0.978999

Nautilus fill：
    1.11 @ 0.979
    8.89 @ 0.980
```

其中第二笔 `8.89 @ 0.980` 是 Nautilus 合成市场状态下产生的补量，不对应第二条真实
OrderFilled。因此，这条路径不能按 Fill-only 的“一笔 source trade 只能消耗有限历史
容量”来理解。

## 4. 静态 L2 + FOK 的实际含义

流程：

```text
signal_ts + 1 秒
    -> 找到到达时刻之前最后一份 L2
    -> 遍历 ask price <= limit
    -> 可见数量足够 10 股
    -> 立即成交
```

脚本创建 `FOK` 限价单后，直接调用 `OrderBook.simulate_fills()`。它没有运行完整订单
生命周期，也没有 freshness 门禁。本样本中：

```text
best ask = 0.979
limit = 0.988 或 0.989
best ask size 约 100 万股
limit 内总 ask depth 至少约 866 万股
```

所以每笔 10 股订单都被模拟为 `10 @ 0.979`。

## 5. 冻结样本

```text
日期：2026-07-08 UTC
Market：Will Switzerland win the 2026 FIFA World Cup?
Outcome：BUY NO
每笔订单：10 shares
模拟延迟：1 秒
TradeTick horizon：30 秒
```

表中的 BUY/SELL 是历史成交的 aggressor side，不是模拟订单方向。

| ID | Signal -> 到达 | Limit | TradeTick GTC | Static L2 FOK |
| --- | --- | ---: | --- | --- |
| 000 | 10:55:19 -> 10:55:20 | .989 | 10:55:23 BUY tick；`1.11@.979 + 8.89@.980` | 盘口年龄 3.933 秒；`10@.979` |
| 001 | 10:55:37 -> 10:55:38 | .988 | 10:55:40 SELL tick；`1.11@.978 + 8.89@.979` | 盘口年龄 16.716 秒；`10@.979` |
| 002 | 10:55:50 -> 10:55:51 | .989 | 10:55:52 BUY tick；`1.11@.979 + 8.89@.980` | 盘口年龄 21.994 秒；`10@.979` |
| 003 | 10:56:08 -> 10:56:09 | .988 | 10:56:11 SELL tick；`1.11@.978 + 8.89@.979` | 盘口年龄 31.544 秒；`10@.979` |
| 004 | 10:56:20 -> 10:56:21 | .989 | 10:56:34 SELL `1022.25@.978`；`10@.978` | 盘口年龄 41.336 秒；`10@.979` |
| 005 | 10:58:28 -> 10:58:29 | .989 | 10:58:31 BUY tick；`1.11@.979 + 8.89@.980` | 盘口年龄 155.064 秒；`10@.979` |
| 006 | 10:58:46 -> 10:58:47 | .988 | 10:58:49 SELL tick；`1.12@.978 + 8.88@.979` | 盘口年龄 173.064 秒；`10@.979` |
| 007 | 10:59:22 -> 10:59:23 | .989 | 10:59:32 BUY `1022.25738@.978999`；`10@.979` | 盘口年龄 168.808 秒；`10@.979` |
| 008 | 11:00:49 -> 11:00:50 | .989 | 未来 30 秒无 tick，`NO_FILL` | 盘口年龄 216.477 秒；`10@.979` |
| 009 | 11:02:01 -> 11:02:02 | .989 | 11:02:22 BUY `1022.25738@.978999`；`10@.979` | 盘口年龄 269.451 秒；`10@.979` |
| 010 | 11:03:19 -> 11:03:20 | .989 | 未来 30 秒无 tick，`NO_FILL` | 盘口年龄 347.451 秒；`10@.979` |
| 011 | 11:05:34 -> 11:05:35 | .989 | 11:05:46 SELL `1022.25@.978`；`10@.978` | 盘口年龄 74.741 秒；`10@.979` |

汇总：

| 模型 | 成交订单 | 成交量 | 总成本 | 均价 |
| --- | ---: | ---: | ---: | ---: |
| TradeTick GTC | 10/12 | 100 | 97.90333 | 0.9790333 |
| Static L2 FOK | 12/12 | 120 | 117.48 | 0.979 |

## 6. 两笔额外成交的来源

额外成交正好是 `real-008` 和 `real-010`。

TradeTick 路径：

```text
signal 后 30 秒内没有任何新成交
    -> 没有事件推动订单撮合
    -> NO_FILL
```

静态 L2 路径：

```text
沿用最后记录的 best ask = 0.979
    -> limit = 0.989
    -> 显示深度远大于 10
    -> 立即全成
```

如果到达时 L2 确实新鲜，那么后续没有历史成交不构成否定证据。模拟 taker 可以主动
吃掉 ask，并创造历史中不存在的反事实成交。这正是 L2 模型相对 TradeTick/Fill-only
合理增加成交机会的机制。

本样本中两张盘口的 exchange age 分别是 216.477 秒和 347.451 秒。年龄本身现在不再
构成拒绝理由；完整 PML2 事件重放也没有看到任何后续失效事件，因此这两笔在当前
event-driven PML2 口径下已经升级为 observed L2 fill。它仍是反事实 taker 回测成交，
不是历史上实际提交并收到交易所 ack/fill 的订单。

## 7. 后续模型必须遵守的口径

```text
已同步且未被后续失效事件撤销的 L2 + marketable limit + 可见深度
    -> 可以产生不依赖未来 TradeTick 的 observed taker fill。

已知 gap / clear / resnapshot-required / handover-unverified / paused / closed
    -> 不允许确定性成交。
    -> 必须等待新的完整 snapshot 或合法状态恢复。

只有时间流逝、没有盘口更新
    -> 不自动过期；安静市场的挂单可以持续存在。

TradeTick-only GTC
    -> 只能证明未来成交事件推动了撮合。
    -> 不能证明合成 L1 补量等于历史真实可用深度。
```

因此，后续比较不能只写“Nautilus 10/12”或“Nautilus 12/12”，必须同时写清：

```text
Nautilus TradeTick + GTC + 30s event replay：10/12
Nautilus arrival-time static L2 + FOK + no freshness gate：12/12
PML2 event-driven validity + explicit invalidation gates：正式研究口径
```
