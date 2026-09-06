# 统一 Fill-only V2/V3 流式回测与 Rust 加速

## 1. 边界

Fill-only V2/V3 只使用：

```text
OrderFilled
  -> maker_fill_ticks
  -> trade_prints_one_sided
  -> versioned Parquet/Arrow trade catalog
```

它不读取 LOB、snapshot、depth 或 queue。V2 的 source-confirmed 成交必须绑定真实的
`trade_prints_one_sided`；概率只能折减或筛选容量，不能创造 source liquidity。V3 的
synthetic、inferred 和 modeled fill 会单独标记，不能当成 observed fill 或实盘成交证明。

PML2 是单独的 L2 对照模型。本文件只统一它们的数据复用、批量执行和性能验收，不会让
Fill-only 读取 L2，也不会把 PML2 描述成纯成交模型。

### 1.1 架构依据

这套方案借鉴的是成熟回测器的数据和运行时边界，不是它们的成交结果：

- [NautilusTrader 回测文档](https://nautilustrader.io/docs/latest/concepts/backtesting/) 将历史数据流、catalog、策略、账户和 Rust 回测内核放在同一系统中，并专门支持批量与重复运行。
- [NautilusTrader #3805](https://github.com/nautechsystems/nautilus_trader/issues/3805) 记录了逐对象、逐字段 PyO3 调用成为主要开销的同类问题；对应解法是列式批量传输，而不是让 Python 逐条跨边界读写。
- [HftBacktest Rust 实现](https://github.com/nkaz001/hftbacktest/blob/master/hftbacktest/README.md) 将 reader、时钟、订单、深度和 queue 状态保留在长寿命 Rust 对象中，说明持久状态比每批重建更符合事件回放。
- [Arrow PyCapsule 规范](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) 提供了跨 Python/本地库传递列式 Arrow 数据的标准协议，是后续减少 NumPy `Vec` 复制的正规升级路径。

`RustPreparedTape`、`RustReplaySession`、V3 evidence-tier router 和 PML2 形状自动路由是本项目针对 OrderFilled/PML2 语义的适配，不是上述项目已有的同名实现。

## 2. 两种研究入口

### VersionedOrderReplay

策略信号和订单先写入带版本、校验和与 source pin 的 order catalog，再由相同 tape
重放。它适合：

- 审计和精确复现；
- V2/V3 或多个 profile 的 A/B 对比；
- Python/Rust 差分；
- 参数敏感性；
- 长周期正式运行和崩溃恢复。

### DynamicStrategyReplay

策略在 `OrderFilled` 事件循环中实现 `on_trade(trade, context)` 并生成订单。`ReplaySession`
保留策略状态、账户、active orders、capacity ledger、RNG、settlement state 和 high-watermark；
每个 chunk 结束后只清理当前市场数据与临时索引。

动态订单会自动绑定触发信号的 `trade_id/tx_hash/log_index`，并排除该笔信号成交，防止用
当前事件证明自己也能成交。`reset_run()` 重置运行状态但保留 immutable catalog；
`snapshot()` 只能在 chunk 边界生成，并绑定 run、family、profile 和 source pin。

```python
from quant.backtest.replay_session import ReplaySession
from quant.backtest.trade_catalog import ParquetTradeCatalog

catalog = ParquetTradeCatalog("/path/to/trade-catalog")
session = ReplaySession(
    run_id="research-001",
    execution_family="V3",
    profile="central_trade_only_l2_reference_expected_fak",
    catalog=catalog,
    random_seed=73,
)
results, receipt = session.replay_dynamic(strategy, chunk_size=100_000)
```

`strategy.on_trade()` 必须返回 `V2TakerOrder` 或 `TradeOnlyOrder`。订单可以交易另一个 token
或 market；框架仍会保留触发事件的 source provenance，并禁止把该事件用于订单自身成交。

## 3. 流式数据层

新 catalog 默认按以下目录物化：

```text
date=YYYY-MM-DD/
  market_id=<id>/
    asset_id=<token>/
      part-XXXXXXXX.parquet
```

manifest 固定：

```text
source pin
profile hash
strategy hash
Arrow schema hash
partition mode
row-group size
每个文件的 checksum、行数、block/time 边界
```

旧的 date-only V1/V2 catalog 仍可只读加载；新建 catalog 使用 V3 manifest 和
`market_asset_date`。读取时先按文件元数据裁剪，再由 Arrow 做列裁剪和 predicate pushdown；
一个 worker batch 只加载所有 profile 所需窗口的并集，并共享同一个 columnar
`PreparedTradeTape`。

chunk 边界不会拆开相同 timestamp、`tx_hash` 或 `trade_group_id`。共享容量、账户或持仓的
订单仍按顺序执行；并行单位是拥有独立 ledger/RNG/account 的 profile，而不是同一容量账本
内的订单。

## 4. V2 与 V3

### V2

V2 是可审计的真实成交流参与模型：arrival 后必须存在满足 side、limit、TIF、latency、
horizon 的 source trade，模拟量受 participation 和共享 capacity ledger 限制。

常用研究分层：

```text
strict/conservative      审计下界
source-confirmed         真实 source evidence，不再重复用无条件概率折减容量
central/probabilistic    OrderFilled proxy 概率筛选
optimistic               宽松敏感性，不作为主收益
```

### V3

V3 是 evidence-tiered trade-only 模型：

```text
A_SOURCE_CONFIRMED       真实未来 source trade
B_TRADE_THROUGH_INFERRED passive trade-through 下界
C_TOUCH_SURVIVAL         touch survival 概率模型
D_SYNTHETIC_ARRIVAL      合成 arrival price/size
E_GENERATIVE_MC          固定 RNG 的生成式成交分布
```

`central_trade_only_l2_reference_expected_fak` 先尝试 source-confirmed；没有 source fill 时
才路由到到达前 OrderFilled 特征的分层概率与条件成交量。
modeled fill 进入独立 `modeled` 账本，不能与 `source_confirmed` 汇总后宣称真实成交。

## 5. HTTP API

服务入口：

```text
GET  /quant/fill-only/v2/profiles
GET  /quant/fill-only/v2/readiness
POST /quant/fill-only/v2/replay
POST /quant/fill-only/v2/resolve-anchor

GET  /quant/fill-only/v3/profiles
GET  /quant/fill-only/v3/readiness
POST /quant/fill-only/v3/replay
POST /quant/fill-only/v3/resolve-anchor
```

请求会严格拒绝未知字段。coverage 不完整或时间锚点无法解析时返回 409，不能转换成
`NO_FILL`。

V2 示例：

```bash
curl -sS -X POST http://127.0.0.1:5000/quant/fill-only/v2/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "requestId": "v2-example-001",
    "profile": "conservative_trade_tape",
    "sourceMaxBlock": 90792478,
    "defaultLookbackBlocks": 100,
    "defaultHorizonBlocks": 100,
    "matcherBackend": "auto",
    "orders": [{
      "orderId": "order-001",
      "marketId": 2416400,
      "assetId": "<token-id>",
      "side": "BUY",
      "limitPrice": "0.60",
      "size": "10",
      "signalBlock": 89502028,
      "signalTs": "2026-07-03T12:00:00Z",
      "tif": "GTD",
      "allowPartialFill": true
    }]
  }'
```

V3 示例：

```bash
curl -sS -X POST http://127.0.0.1:5000/quant/fill-only/v3/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "requestId": "v3-example-001",
    "profile": "central_trade_only_l2_reference_expected_fak",
    "sourceMaxBlock": 90792478,
    "defaultLookbackBlocks": 100,
    "defaultHorizonBlocks": 100,
    "randomSeed": 73,
    "matcherBackend": "auto",
    "orders": [{
      "orderId": "order-001",
      "marketId": 2416400,
      "assetId": "<token-id>",
      "side": "BUY",
      "limitPrice": "0.60",
      "size": "10",
      "signalBlock": 89502028,
      "signalTs": "2026-07-03T12:00:00Z",
      "tif": "GTD",
      "liquidityIntent": "TAKER"
    }]
  }'
```

若只有时间戳，先调用 `resolve-anchor`，或在 replay order 中省略 `signalBlock`；服务会使用
同一份 pinned OrderFilled tape 解析锚点。

## 6. 长周期 runner

单 profile 日常研究：

```bash
python scripts/run_unified_fill_only_replay.py \
  --family V2 \
  --trade-catalog /path/to/trades \
  --order-catalog /path/to/orders \
  --output /path/to/results \
  --run-id study-001 \
  --profiles probabilistic_trade_tape \
  --workers 1 \
  --chunk-mode signal_day \
  --chunk-days 1 \
  --recycle-batches 5 \
  --matcher-backend auto
```

多 profile 共享数据：

```bash
python scripts/run_unified_fill_only_replay.py \
  --family V3 \
  --trade-catalog /path/to/trades \
  --order-catalog /path/to/orders-v3 \
  --output /path/to/results-v3 \
  --run-id study-v3-001 \
  --profiles taker_source_confirmed,central_trade_only_l2_reference_expected_fak,generative_tape_mc \
  --workers 1 \
  --matcher-backend auto
```

追加 `--resume` 从原子 checkpoint 恢复。正式 cold-cache 基准追加 `--evict-os-cache`；该参数
要求 Linux `posix_fadvise`，任意文件 eviction 失败都会终止，不会伪装 cold run。

输出使用有界队列批量写 Parquet。research 模式保存精简结果；`--audit` 保存完整 source-fill
字段。checkpoint 记录结果 high-watermark 和增量 ledger delta，不会每天重新哈希全部历史
ledger。

## 7. Rust 后端

仓库通过根目录 `rust-toolchain.toml` 固定 Rust `1.85.1`，通过
`rust/fill_only_kernel/pyproject.toml` 固定 maturin `1.8.3`。在目标 Python 环境中构建：

```bash
cd rust/fill_only_kernel
python -m maturin develop --release
```

`matcherBackend=auto` 在语义受支持且扩展可用时选择 Rust，否则明确记录 fallback 原因并走
Python。`matcherBackend=rust` 不允许静默 fallback。

Rust 后端分成两个层次，不能混写为“所有 V2/V3/PML2 已经 Rust 化”：

| 路径 | Rust 职责 | Python 职责 | 当前边界 |
| --- | --- | --- | --- |
| V2 source-confirmed | 窗口搜索、side/limit/TIF、容量分配 | 概率特征、合同对象与审计结果 | 只支持兼容子集 |
| V3 source-confirmed | 复用 V2 Rust | evidence-tier 结果转换 | 可用 |
| V3 central/hierarchical | source-confirmed 子路径复用持久 Rust session | 特征、概率、路由与 modeled ledger | hybrid，当前主研究路径 |
| V3 generative MC | 固定 RNG Monte Carlo | 特征与结果合同 | 可用 |
| PML2 independent snapshot FAK/FOK/IOC | 大批量浅盘口 book walk | 小批量 book walk、合同与审计结果 | `auto` 按实测批次形状选 Python/Rust |
| PML2 dynamic delta/queue/GTD | 无 | 完整事件驱动 session | Python `CHAIN_ONLY` 实测路径；当前 profiling 不支持迁 Rust |

持久化 Rust 路径采用粗粒度对象：

```text
RustPreparedTape
    trade columns + market/asset/side offsets（一次构造）
        ↓
RustReplaySession
    source capacity + RNG/high-watermark（跨 batch 保留）
        ↓
replay_batch(order columns)
```

同一进程内的多个 profile 才能共享同一个 native tape。把三个 profile 分给三个独立 worker
会各自构造一份 Rust tape；多进程共享必须通过 Arrow mmap/catalog 共享输入页，而不是宣称共享
同一个 Rust 对象。

金额和数量使用 10 位小数固定精度。任何新 native 路径都必须与 Python 的 result、source
allocation 和 ledger 逐字段一致；不兼容路径继续明确回退 Python。

PML2 的 `auto` 不是“Rust 优先”。当前同机分界基准显示，小批量或多档成交时 NumPy/PyO3
物化成本会抵消 Rust 热循环收益。因此只有订单数至少 `2,000` 且每单最多物化 4 个可消费档
时选择 Rust；其他形状走更快的批量 Python。强制 `rust` 仍可用于差分验证。

## 8. 性能测量合同

性能报告必须把下列阶段分开：

```text
source discovery / database query
L2 or trade catalog materialization
Arrow/Python object conversion
prepared tape/index construction
V3 matching and result construction
PML2 matching and result construction
result serialization
end-to-end cold / warm elapsed
```

2026-09-01 的 11,000 单旧基线显示：十个窗口总耗时 `1686.29s`，其中 V3 两条中央路径
`18.71s`、PML2 `87.26s`、Nautilus control `10.29s`，其余约 `1570s` 是数据发现、L2
读取、订单构造和输出。这个基线否定了“只重写 matcher 就会让端到端大幅加速”的说法。

因此实现顺序固定为：

1. 版本化 prepared-input cache，warm run 不重复查询、解析和重建相同输入；
2. Rust tape/index/session 持久化，避免每批复制 trade columns 和重建分组；
3. PML2 独立 snapshot FAK/FOK/IOC 批量 book-walk，并裁剪到订单实际可能消费的最短盘口前缀；
4. 对 V3 central 做 profile，只有其自身仍占显著比例时才继续迁移；
5. dynamic PML2 先使用按 condition 的等待订单索引和 `CHAIN_ONLY` 审计；只有纯撮合事件循环重新成为主要瓶颈后才迁移 Rust。

验收同时报告纯模型、warm end-to-end 和 cold end-to-end。热循环倍率不能代替完整回测倍率。

## 9. 验证与性能门

功能验证：

```bash
python -m pytest -q \
  quant/backtest/tests/test_replay_session.py \
  quant/backtest/tests/test_trade_catalog.py \
  quant/backtest/tests/test_profile_worker.py \
  quant/backtest/tests/test_parquet_result_writer.py \
  quant/backtest/tests/test_rust_kernel.py \
  quant/backtest/tests/test_trade_only_v3.py \
  quant/backtest/tests/test_fill_only_v2_api.py \
  quant/backtest/tests/test_fill_only_v3_api.py
```

性能门使用 `scripts/validate_unified_fill_only_performance.py` 读取版本化 receipt。它验证：

- 30/60/173 天 golden result 与 ledger hash；
- 工作量可比的 `T60/T30 <= 2.2`；
- 前后 10 天按百万 indexed trade rows 归一化后无持续退化；
- RSS、primary、六 profile warm/cold 上限；
- cold-cache eviction 证据；
- warm/cold 每个 profile hash 一致；
- V2 Rust/Python 等价和至少 3x 热循环加速；
- PML2 Python/Rust 等价，`auto` 必须选择同机实测更快的后端；
- V3 indexed scan reduction 和真实 100-order hash。

固定前 30 天与前 60 天的原始时间比也会输出，但单列为
`DENSITY_MISMATCH_DIAGNOSTIC`：它不会用工作量更密集的自然月份伪装算法超线性，也不会被
隐藏成通过结果。

## 10. V3/PML2 万人级性能验收

版本化输入覆盖 10 个互不重叠窗口、5 个日期、366 个 market、469 个 market-day、
`0.011-0.999` 全价格区间，共 12,500 笔订单。当前五模型任务包括 V3 source、V3 两条中央
路径、PML2 和 Nautilus control。

| 指标 | 旧实现 | 当前实现 | 变化 |
| --- | ---: | ---: | ---: |
| 完整任务 wall time | 3,265.92 秒 | 57.90 秒 | 56.41x |
| V3 expected | 11.08 秒 | 5.24 秒 | 2.11x |
| PML2 | 94.26 秒 | 3.46 秒 | 27.27x |
| 当前 peak RSS | - | 348,028 KiB | < 6 GiB 门槛 |

PML2 公平同进程差分中，12,500 笔订单包含 529,199 个原始单边盘口档；可成交前缀只有
9,379 档。批量 Python 为 `1.5378s`，裁剪后 Rust 为 `1.6496s`，所以十个 1,250 单窗口的
`auto` 正确选择 Python。2,000 单浅盘口微基准中 Rust 为 Python 的 `1.215x`，达到路由条件
时才会自动选择 Rust。

最终 run 与改动前当前-artifact run 的五个模型逐单差异均为 0。结果哈希：

```text
v3_source_fak     56a67db30ad084871dd675de034162b5d88a8758c836a22209dc990fdf316d04
v3_l2_reference   49bc2a8be598ee8a5cc361919e971ba52d62e3c71593d513f681b31d35da48b0
v3_l2_expected    fcbfe5aaaec366003ec3a34b2a061cdf0dccac7389149f90e8bf007dfb59b6ed
pml2_fak          2a5b12fd02ad306c3102708d3effb277f921fabae933c260684dd419a5556462
nautilus_fak       193f24e5d10b39d8985b147511bd64fed8430424d443752e556c56abece74192
```

完整产物位于：

```text
backtest_framework/nautilus_trader_comparison/rust_acceleration_validation/
  cross_validation_12500_final/
```

性能和等价性通过不等于概率模型质量通过。随后启用的 Platt ridge artifact 在另一组
12,276 单、144 market、159 market-day 的外部验证中为
`PASS_WITH_CALIBRATION_WARNINGS`：支持域 11,993 单，支持率 `97.69%`；相对 PML2 的订单量比
`95.89%`、成交量比 `96.66%`，Brier `0.20531`，优于对应 climatology `0.21806`。
仍有一个小时窗口及 finance/tech 分层存在正 Brier regret，高活跃度 `>50` 区间因时间漂移
继续 abstain，不能将 aggregate PASS 写成每个局部分层都已校准。

## 10.1 PML2 dynamic delta/queue/GTD

动态路径使用真实 compact L2 研究语料，覆盖 50 个 market、16 个类别、78,393 条
snapshot/delta/trade 事件和 11,000 笔 GTD maker 订单。compact cache 不包含完整 raw-frame
proof，因此这里只证明性能与逐单等价，不证明正式归档 coverage。

旧实现的主要慢点不是 maker queue：

```text
每个事件 -> 重新哈希整本 residual book
每个 local delivery -> 扫描本次 run 的全部订单寻找 WAITING_FOR_DATA
```

修正后：

- `FULL` 保留原逐事件全状态审计，默认和正式 API 语义不变；
- `CHAIN_ONLY` 保存长度前缀 SHA-256 事件链、增量 match hash 和最终 execution-state hash；
- 等待新鲜数据的订单按 condition 建有序索引，空集合不再扫描全部订单；
- snapshot、delta、trade、queue、GTD、双时钟和撮合结果均未修改。

同一 60 单/1,249 事件控制中，`FULL 14.390s -> CHAIN_ONLY 0.252s`，为 `57.11x`；
逐单结果和最终盘口哈希完全一致。万人级结果：

| 模式 | run time | 总时间（含 ingest/submit） | 逐单差异 |
| --- | ---: | ---: | ---: |
| `CHAIN_ONLY` | 20.080 秒 | 24.867 秒 | 0 |
| 无审计诊断控制 | 14.333 秒 | 18.929 秒 | 0 |

`CHAIN_ONLY` 的 5.75 秒差额是保留滚动审计链的明确成本。无审计后的完整 Python 动态热循环
仅约 14 秒，当前没有证据表明将 delta/queue/GTD 搬入 Rust 会产生数量级端到端收益，因此
readiness 标记为 `PYTHON_PROFILED / NOT_JUSTIFIED_BY_CURRENT_PROFILE`。未来只有在更高 trade
密度或更多 active maker queue 的新 profiling 中，纯事件循环持续占总耗时至少 30% 时才重开
Rust 迁移。

PML2 V2 HTTP 研究请求可显式传入：

```json
{"auditMode": "CHAIN_ONLY"}
```

省略时仍为 `FULL`，未知字段仍返回 HTTP 400。基准与 `cProfile` 文件位于
`backtest_framework/nautilus_trader_comparison/pml2_dynamic_profile/`。

主策略回测入口 `POST /quant/backtest-runs` 已接入同一参数：

```json
{
  "executionPriceMode": "PREDICTION_L2_REPLAY_V1",
  "pml2AuditMode": "CHAIN_ONLY"
}
```

主回测属于本地研究工作流，默认使用 `CHAIN_ONLY`；需要保存每个输入事件完整审计 payload 的
正式复核运行应显式传 `FULL`。该值写入 `quant.quant_backtest_parameters`、参数快照和 fingerprint，
恢复执行不会改变模式。两种模式的订单、成交、队列和最终盘口状态相同，差别仅是逐事件审计的
存储与哈希成本。独立 `/quant/prediction-l2/v2/replay` 继续默认 `FULL`。

## 11. V2 173 天验收结果

版本化输入：

```text
source pin     ab84349c9a3c5cdfc46e7cf7f6b83d240cf38f69567be338db86971a1b2b2598
trade rows     92,268,393
orders         372,479
days           173
profiles       6
ClickHouse     0 queries during catalog replay
```

结果：

| Gate | 实测 | 门槛 |
| --- | ---: | ---: |
| 15 天 primary | 66.47 秒 | 阶梯点完成 |
| 173 天 primary warm | 1,277.29 秒 | < 5,400 秒 |
| 六 profile warm | 2,904.75 秒 | < 10,800 秒 |
| 六 profile cold | 2,881.84 秒 | < 14,400 秒 |
| cold eviction | 534 files / 7.19 GB | 非空且无错误 |
| peak worker RSS | 4,775,492 KiB | < 6,291,456 KiB |
| normalized last10/first10 | 1.0240 | <= 1.10 |
| workload-controlled T60/T30 | 1.7033 | <= 2.20 |
| V3 source scan reduction | 99.80% | >= 90% |
| Rust median speedup | 4.6966x | >= 3x |

Warm/cold 六个 profile 的 result hash 和 ledger hash 逐一相同。完整机器验收保存在：

```text
runtime_outputs/unified_fill_only_acceleration/acceptance/performance_acceptance_v1.json
```

固定前缀的原始 `T60/T30=2.5306` 没有通过 2.2 参考线，因为前 60 天包含约 2.56 倍的
indexed trade rows；它作为 density diagnostic 保留，不替代上表的工作量可比扩展门。

## 12. 当前完成状态与剩余验收

不能把“核心执行链可用”写成“所有回测工作全部完成”。当前状态如下：

| 模块 | 当前状态 | 可使用边界 |
| --- | --- | --- |
| Fill-only V2 | 数据切片、容量账本、TIF、审计、Rust 兼容子集、API 和主引擎路由已完成 | 保守、source-confirmed 本地研究回测 |
| Fill-only V3 | 概率成交、分层先验、Monte Carlo、中央路由、独立 API、统一主入口和万人级验证已完成 | 本地概率研究；modeled fill 不得写成 observed fill |
| PML2 | snapshot/delta/trade、FAK/FOK/GTD、Maker queue、coverage、API 和主引擎路由已完成 | 有逐请求 L2 coverage 证据的研究回测 |
| Rust 加速 | V2 source、V3 source/MC、PML2 snapshot taker 已接入 | 根据 profiler 自动选择；不追求无收益的全 Rust |
| 金融结算 | Oracle 结果、cutoff、取消/未结算分类、组合 finalizer 和 API 已完成 | 未结算或证据不足的 market 必须输出 partial/不可结算 |
| 统一主入口 | V2、V3、PML2 均已接入 `/quant/backtest-runs` 和前端执行模式 | 三类执行模型可用同一策略订单与金融 finalizer |

当前尚未满足的内容：

1. V3 没有真实提交订单终态标签，`ready_for_live_transfer_claim` 必须保持 false。
2. PML2 Maker survival 的自有订单 holdout 数量和结果多样性不足，modeled 结果不能进入 observed PnL。
3. Fill-only 派生数据与 XUE L2 都只能在已证明覆盖的请求窗口使用，区间外必须 fail closed。
4. 全历史 coverage 和生产迁移门仍是外部数据任务，不能用模型参数绕过。

本地研究回测完成门：

```text
V2 / V3 / PML2 均可从统一主入口选择
策略信号 -> order intent -> execution -> ledger -> financial finalizer 可贯通
未知参数返回 HTTP 400
observed / modeled / abstain 分账
相同输入在 Python/Rust、chunk size、恢复执行下逐单一致
至少 10,000 单跨日期、market、类别、价格和流动性验证
focused tests、Ruff、MyPy、HTTP/DB canary 通过
backtest_core_gate = ready
```

当前本地研究门的运行证据：

```text
structural backtest_core_gate         ready: 7 ready / 0 review / 0 fail
DB latest-run stage gate              review: 8 ready / 1 review / 0 fail
V3 active probability artifact       promoted_local_research
Aug 06-08 primary                    12,276 orders / PASS_WITH_CALIBRATION_WARNINGS
Jul 21-24 broad cross-check          11,250 orders / PASS_WITH_CALIBRATION_WARNINGS
Aug 07-08 active supplement          activity-regime gates 3/3
combined V2/V3/PML2/Rust/finance     411 tests passed
HTTP modeled canary run 132          expected = 8.5223457686, actual = 0, ledger = 0
HTTP source canary run 134           expected = actual = 0.2635, ledger = 1
run 134 source evidence              69bc7cd92799e34871fa5073aac652b8cc6e710df63607cf636bc919852ae8dd
run 134 artifact core                slice/TIF/probability/ledger/parity all ready
modeled financial finalizer          N/A_NO_DEPLOYED_CAPITAL
source financial finalizer           FINANCIAL_PARTIAL at 2026-08-25 cutoff
```

`PASS_WITH_CALIBRATION_WARNINGS` 表示总体、窗口和至少 80% 的类别/价格/活跃度覆盖门通过，
不是逐订单真实成交保证。V3 readiness 因没有真实提交订单标签而继续拒绝 live-transfer 声明。

数据库 stage gate 的唯一 `review` 不是执行合同失败。它来自 run 134 仅有一笔订单、两行信号价格、
没有平仓 trade，且目标 market 在 cutoff 时仍为 OPEN；因此 tail risk、完整组合收益和实盘校准没有
足够样本。该门保持 `review` 是正确行为，不能通过伪造平仓、结算或真实订单标签改成 `ready`。

V3 主入口的财务口径为：

```text
MODELED_EXPECTATION
    只写 expected_fill_size / expected_fill_notional
    不写标准现金账和持仓
    finalizer 返回 N/A_NO_DEPLOYED_CAPITAL

A_SOURCE_CONFIRMED
    actual_fill_size 才写标准现金账和持仓
    source_trade_ids 必须可追溯
    finalizer 只结算 actual position
```

生产或实盘迁移门独立于本地研究完成门：

```text
V3 至少 200 个真实提交订单终态标签，且 fill/no-fill 各不少于 20
Maker holdout 同时包含 FULL/PARTIAL/NO_FILL 并通过 walk-forward
price buffer 具有足够真实 markout 样本
请求窗口具备真实数据 coverage proof
ready_for_live_transfer_claim = true
```

外部标签或历史数据不足时不得通过放宽门槛、回填假标签或把 `NO_FILL` 改名来伪造完成。
