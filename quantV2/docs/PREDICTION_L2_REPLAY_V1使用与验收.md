# PREDICTION_L2_REPLAY_V1 使用与验收

## 1. 模型定位

`PREDICTION_L2_REPLAY_V1` 是独立的预测市场 L2 回放模型，不是
Fill-only V2/V3，也不是旧的 `ORDERFILLED_LOB`。

```text
Fill-only V2/V3
    用真实后续成交流约束反事实订单；没有合格 source trade 时不成交。

PREDICTION_L2_REPLAY_V1
    订单到达交易所时直接消费当时有效的 L2 经济盘口；
    Taker 不要求未来出现 OrderFilled。

LEGACY_ORDERFILLED_LOB
    保留旧语义，只用于历史结果回归。
```

当前成熟度是：

```text
RESEARCH_GRADE
```

不能称为生产级真实成交复原。当前已经接入既有 shadow/live 标签和
walk-forward Maker 生存模型，但自有真实订单 holdout 只有 4 条且全部为
`NO_FILL`，不足以完成从 OrderFilled proxy 到真实下单成交概率的校准迁移。

## 2. 核心成交语义

每次 run 只创建一个 `ReplayExecutionSession`，其中持续保存：

```text
ExchangeBookState
ObservedBookState
EconomicResidualBook
EconomicMakerQueueLedger
TradeDeltaReconciler
OrderStateStore
Audit hash chain
```

策略只能看到 `local_ts` 已到达的 `ObservedBookState`；订单在
`exchange_arrival_ts` 使用 `ExchangeBookState` 撮合。

YES/NO 盘口先映射到 canonical YES 坐标：

```text
YES BID p = NO ASK (1-p)
YES ASK p = NO BID (1-p)
```

所以 `BUY YES @ 0.60` 与 `SELL NO @ 0.40` 会消费同一个经济档位，不能
重复使用镜像容量。

### Taker

```text
observed signal
→ entry latency
→ exchange arrival
→ market/trading mode/book validity/tick/订单金额与 venue-admission 检查
→ optional venue delay
→ 在延迟结束时重新检查盘口
→ 按 limit 多档 walk
→ 原子更新 run-scoped residual
```

每个 child fill 始终满足：

```text
BUY fill_price <= limit_price
SELL fill_price >= limit_price
```

不会在撮合后再加滑点并突破 limit。保守性由 latency、L2 容量 haircut、
显式盘口失效事件和 profile 表达。

#### Polymarket 原生订单金额合同

PML2 不再把所有订单的 `size` 都解释为 shares。请求必须显式携带
`amount_unit`，老请求继续默认为 `SHARES`：

```text
BUY + QUOTE + FOK/FAK/IOC
    amount 是最多可花费的 quote/pUSD。
    按 ask 从低到高逐档消耗，每档成交 shares = quote_remaining / price。

SELL + SHARES + FOK/FAK/IOC
    amount 是卖出的 outcome shares。
    按 bid 从高到低逐档消耗。

GTC/GTD/post_only
    必须使用 SHARES，因为剩余订单要进入以 shares 计量的 Maker queue。

BUY + SHARES
    作为既有限价策略回放的向后兼容合同保留；
    历史 Polymarket market BUY 对账应优先使用 QUOTE。
```

真实签名订单可额外携带已换算为人类单位的
`signed_maker_amount` 和 `signed_taker_amount`：

```text
BUY:  signed_maker_amount = quote budget
      signed_taker_amount = signed worst price 下的最小 shares

SELL: signed_maker_amount = shares
      signed_taker_amount = quote proceeds at signed price
```

两个 signed amount 必须同时出现。它们出现时，撮合使用真正签名的
maker amount 作为可执行金额，不会用原始 UI 输入反推 shares。BUY 获得
价格改善时，实际 shares 可大于 `signed_taker_amount`。

费用始终是成交后的独立账本项：

```text
quote budget 只用于买入 shares
fee 不会从 quote budget 再扣一次
filled_notional = sum(fill_qty * fill_price)
remaining_amount = requested_effective_amount - filled_notional  # QUOTE BUY
```

FOK 的完整性按订单的 amount unit 判断：QUOTE BUY 必须能在限价内消耗
全部 quote budget，SHARES 订单必须能成交全部 shares；否则 0 fill，
L2 residual 不变。FAK/IOC 允许部分成交并取消剩余 amount。

该合同直接对应 Polymarket 官方实现：market BUY 的用户输入 `amount` 是
quote/pUSD，SDK 先按真实 asks 的累计 notional 求最差可接受价格，再构造
BUY `makerAmount=quote`、`takerAmount=quote/price` 的签名订单；交易所把价格
改善归给 taker。因此回放既保留签名的最小 shares，又以 BUY maker amount
作为订单剩余量，不能把 UI quote 金额静默改成 shares。依据：

- [Polymarket Order Lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
- [Polymarket CLOB V2 order builder](https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/order_builder/builder.py)
- [CTF Exchange matching overview](https://github.com/Polymarket/ctf-exchange/blob/main/docs/Overview.md)

#### `min_order_size` 与交易所接受证据

`min_order_size` 是 admission metadata，不是 L2 成交容量。普通反事实订单默认
`venue_admission=UNKNOWN`，使用到达时快照的 minimum 严格检查：

```text
SHARES          -> share amount
QUOTE BUY       -> signed_taker_amount（如有），否则 quote / limit_price
```

回放自有真实订单时，可以传入：

```text
venue_admission = ACCEPTED
    只表示该历史订单已被 CLOB 接受。
    即使快照 min_order_size 与它冲突，仍继续撮合，
    并记录 MIN_ORDER_SIZE_CONTRACT_CONFLICT。

venue_admission = REJECTED_MIN_ORDER_SIZE
    按已观察到的交易所结果拒绝。

venue_admission = UNKNOWN
    不允许猜测，仍按快照 minimum 严格拒绝。
```

`ACCEPTED` 不是普通策略回测的放宽开关，必须来自可审计的历史下单响应。
任何非 `UNKNOWN` 值都必须同时提供 `venue_admission_evidence_id`，否则请求
直接拒绝。
这样可以重放“交易所确实接受的 1/2-share 订单”，又不会把未知的小单
无条件设为可成交。

盘口有效性采用事件驱动规则：完整 snapshot 建立的状态持续有效，直到 gap、
clear/reset、要求重新同步的 lifecycle、未验证 handover、暂停或关闭明确使其失效。
仅仅一段时间没有 book/delta 不会让盘口自动过期；`max_book_age_ms` 只保留给
`MAX_AGE` 历史差分控制，不是默认正式口径。

### TIF

```text
FOK   容量不足时 0 fill，residual 不变化
FAK   成交可执行部分，剩余取消
GTC   marketable 部分先成交，剩余进入 Maker queue
GTD   与 GTC 相同，但到期自动取消
post_only  到达时 crossing 则拒绝
```

### Maker

Maker 订单进入 canonical economic level 的共享 FIFO queue。只有正确方向的
trade print 才推进；trade 与匹配的 L2 decrease 先经
`TradeDeltaReconciler` 去重，避免同一外部成交推进两次。

XUE-LAB Native L2 冷归档会先恢复 YES/NO baseline，再按完整顺序自动物化
`book`、`price_change` 和 `last_trade_price`。因此 Maker 回放可直接获得
snapshot、delta 和 trade 时间线，不再要求调用方手工拼接 Maker 事件。

Maker 生存概率使用既有校准资产构建版本化、hash-pinned 的
`interval_censored_discrete_survival_walk_forward_v1`。独立统计单位是 Maker
trial，按 30/120/300/900 秒累计成交概率处理右删失，并按
category/side/quote-position/queue-bucket 分层；小样本会向全局先验收缩。
它不是 Cox/KM，也不会在校准训练期之前回填预测。当前真实自有订单 holdout
不足，所以概率输出明确标记：

```text
ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED
```

## 3. HTTP API

```text
GET  /quant/prediction-l2/v1/profiles
GET  /quant/prediction-l2/v1/readiness
POST /quant/prediction-l2/v1/replay
POST /quant/prediction-l2/v1/replay-matrix
POST /quant/prediction-l2/v1/maker-forecast
```

JSON 使用严格字段校验；未知字段返回 HTTP 400。

### 3.1 显式事件回放

```json
{
  "request_id": "example-1",
  "run_id": "pml2-example-1",
  "profile": "realistic",
  "events": [
    {
      "type": "SNAPSHOT",
      "snapshot_id": "snapshot-1",
      "condition_id": "0xcondition",
      "market_id": "market-1",
      "asset_id": "yes-token-id",
      "outcome": "YES",
      "exchange_ts": "2026-07-08T10:53:23Z",
      "local_ts": "2026-07-08T10:53:23.100Z",
      "book_epoch": 0,
      "source": "native_l2",
      "is_full_depth": true,
      "tick_size": "0.01",
      "min_order_size": "1",
      "bids": [{"price": "0.87", "size": "20"}],
      "asks": [{"price": "0.88", "size": "10"}]
    }
  ],
  "orders": [
    {
      "order_id": "order-1",
      "strategy_id": "strategy-1",
      "condition_id": "0xcondition",
      "market_id": "market-1",
      "asset_id": "yes-token-id",
      "outcome": "YES",
      "side": "BUY",
      "size": "5",
      "amountUnit": "SHARES",
      "limit_price": "0.88",
      "tif": "FAK",
      "signal_ts": "2026-07-08T10:53:24Z"
    }
  ]
}
```

Market BUY 的原生 quote-budget 请求示例：

```json
{
  "side": "BUY",
  "size": "1.08",
  "amountUnit": "QUOTE",
  "signedMakerAmount": "1.08",
  "signedTakerAmount": "40",
  "limitPrice": "0.027",
  "tif": "FOK",
  "venueAdmission": "ACCEPTED",
  "venueAdmissionEvidenceId": "historical-clob-response:order-id"
}
```

API 中 signed amounts 使用已换算的 quote/shares 十进制单位，不是合约里
`1e6` 缩放后的整数。未知字段仍返回 HTTP 400。

### 3.2 从 XUE-LAB Native L2 归档按时点恢复

`archive_restore.start_time` 必须严格早于该 condition 最早订单的交易所到达
时间。API 会恢复 baseline，并在每个订单到达和 venue-delay 结束前重建
point-in-time baseline 和随后有序的 book/delta/trade 事件；不会使用未来
snapshot 初始化过去订单。

```json
{
  "run_id": "pml2-cold-example",
  "profile": "strict",
  "archive_restore": [
    {
      "condition_id": "0xcondition",
      "market_id": "market-1",
      "yes_asset_id": "yes-token-id",
      "no_asset_id": "no-token-id",
      "start_time": "2026-07-08T10:53:20Z",
      "end_time": "2026-07-08T10:58:23Z",
      "maker_horizon_seconds": 300,
      "book_epoch": 0
    }
  ],
  "orders": [
    {
      "order_id": "order-1",
      "strategy_id": "strategy-1",
      "condition_id": "0xcondition",
      "market_id": "market-1",
      "asset_id": "yes-token-id",
      "outcome": "YES",
      "side": "BUY",
      "size": "1",
      "limit_price": "0.88",
      "tif": "FAK",
      "signal_ts": "2026-07-08T10:53:23Z"
    }
  ]
}
```

归档目录优先从 `PML2_L2_ARCHIVE_DIR` 读取，否则使用 XUE-LAB Native L2
挂载。该数据源按项目合同被视为唯一权威且完整，不建设 PMXT fallback 或
hot/cold event-hash 对账。挂载不可用、时点无基线、事件上限截断或历史费率
缺口仍返回 HTTP 409，不会退化为空盘口或 `NO_FILL`。

“假定完整”是数据源合同，不等于每次请求都证明了全历史覆盖。每个 run 只为
`archive_restore` 指定的窗口出具恢复收据，并要求：YES/NO baseline 都保存
`exchange_ts/received_ts`、checkpoint 不晚于 PIT cutoff、raw frame/group
完整性证据存在。缺少这些双时钟或 frame 证据时返回 HTTP 409；不会构造一个
合成 exchange baseline 来放行。成功响应会写出 `clock_verified`、
`clock_evidence`、`baseline_clock_count`、`raw_event_clock_count` 和
`frame_evidence_verified`。

### 3.3 effective-dated fee schedule

请求可带 `fee_schedules`。撮合在实际 fill time 选择有效费率，不用当前费率
回填历史。无效配置返回 400；提供 registry 后却覆盖不到 fill time 返回 409。

```json
{
  "schedule_id": "fee-regime-2026-01",
  "asset_id": "yes-token-id",
  "condition_id": "0xcondition",
  "effective_from": "2026-01-01T00:00:00Z",
  "effective_until": "2026-08-01T00:00:00Z",
  "platform_fee_rate": "0.02",
  "platform_fee_exponent": "1",
  "platform_taker_only": true,
  "source": "historical-market-config"
}
```

### 3.4 三档敏感性

将同一请求发送到 `/replay-matrix`，服务会在完全相同的事件与订单上分别运行：

```text
strict
realistic
optimistic
```

三个 profile 的 `depth_haircut`、补单比例、延迟、book validity、镜像政策和
Maker queue 假设来自：

```text
config/execution/pml2_profiles.v1.json
```

这些配置当前标注为研究默认值，尚不是由自有真实订单训练出的生产参数。
`realistic.depth_haircut=1.00` 按 Polymarket 的即时扫盘语义消耗到达时的全部
可见 residual；延迟、限价、TIF、coverage 和共享残量继续约束成交。
`strict.depth_haircut=0.50` 仅作为深度保守敏感性，不再把未校准的 20% 扣减
混入中央协议口径。

### 3.5 Maker 生存概率

```json
{
  "decision_ts": "2026-08-01T00:00:00Z",
  "horizon_seconds": 300,
  "category": "sports",
  "side": "BUY",
  "quote_position": "AT_BEST",
  "queue_bucket": "Q2_LE_5X",
  "minimum_stratum_samples": 20
}
```

响应同时返回分层样本数、raw probability、global prior、收缩权重、最终
fill probability、训练窗口、artifact hash 和 transfer-validation 状态。
如果 `decision_ts` 不晚于训练结束时间，返回
`CALIBRATION_NOT_POINT_IN_TIME_AVAILABLE`，防止校准信息穿越。

### 3.6 多腿执行协议

`order_groups` 支持：

```text
SEQUENTIAL
SIMULTANEOUS_BEST_EFFORT
ALL_LEGS_FOK
HEDGE_ON_LEG_FAILURE
```

`ALL_LEGS_FOK` 要求每条 primary leg 都是 FOK。引擎先在同一份共享经济残量
上预演全部腿；任意腿不能全成时整组 0 fill、残量不变，全部通过后才一次性
原子提交。`HEDGE_ON_LEG_FAILURE` 不会暗中假设无摩擦对冲：primary leg 失败
后只会提交请求中预先声明的 `hedge_legs`，并对它们独立应用延迟、限价、深度
和手续费。

大额 Taker 订单还会计算（QUOTE BUY 使用 quote notional，其他订单使用
shares）：

```text
order_amount / visible_amount_within_limit
```

该 ratio 只是反事实冲击警告，不是一个任意的 `2x/3x/5x` 拒绝门。
FAK/IOC 仍只能消费真实可见 residual，剩余取消；FOK 在可见深度不足时
依其原子语义 0 fill。模型不会通过价格冲击公式或内生市场响应创造新盘口。

## 4. 动态策略回放

`DynamicPredictionReplay` 支持策略随本地可见事件动态发单，不需要先生成半年
全部订单。策略回调包括：

```text
on_book
on_trade
on_external_event
on_order_update
on_resolution
```

上下文只暴露 `ObservedBookView`、已观察 OMS 和因果可用事件。策略若使用未来
时间提交订单会被拒绝。

```python
from quant.backtest.pml2 import DynamicPredictionReplay, ReplayExecutionSession

session = ReplayExecutionSession(run_id="dynamic-1", profile="realistic")
runner = DynamicPredictionReplay(session=session, strategy=my_strategy)

runner.ingest_snapshot(snapshot)
runner.ingest_trade(trade)
runner.run()
```

`BOOK_LEVEL_BATCH` 在 exchange state 和 observed state 中都原子应用，策略不会
看到 batch 中间态。

## 5. 金融记账

所有成交输出统一 `ExecutionMatch`，包含：

```text
raw/canonical side and price
exchange arrival/fill/receive timestamps
book epoch and snapshot id
source event ids
residual and queue transitions
fee schedule id/source
finality state
audit hash
```

`financial_finalization_service.py` 通过 adapter registry 同时接收：

```text
PREDICTION_L2_REPLAY_V1
Fill-only V3
OrderFilled V2
Legacy ORDERFILLED_LOB
```

金融域已支持 `MATCHED → PENDING/MINED/RETRYING → CONFIRMED/FAILED`、
`COMPLEMENTARY/MINT/MERGE` 标注、split/merge/redeem、0.5/0.5 payout vector、
显式 negative-risk conversion 和幂等记账。链上 position operation 的
allowance/nonce/submitted/mined/confirmed 状态机复用现有
`quant.simulator.operations`，不把链上操作伪装成零时间普通成交。

## 6. 输出审计

主响应包含：

```text
execution_matches
orders and status reasons
run-scoped residual snapshot
mirror mismatches
order group results and leg statuses
Maker survival forecast and artifact hash
counterfactual large-order gate evidence
replay_hash
coverage_manifest
data_source_plan
```

`coverage_manifest.data_quality_status` 只有三种：

```text
VALID
VALID_WITH_DEGRADATION
REJECTED
```

gap、显式 stale/reset、无基线、未验证 source handover 都 fail-closed。截断深度允许 FAK
消费已知可见容量，但不能证明 FOK 的完整容量。

## 7. 验证命令

核心语义与 API：

```bash
conda run --no-capture-output -n polyBacktest \
  pytest -q quant/backtest/tests/test_prediction_l2_replay_v1.py
```

真实本地归档只读 smoke：

```bash
conda run --no-capture-output -n polyBacktest \
  python scripts/validate_prediction_l2_replay_v1.py \
  --output runtime_outputs/pml2_validation/real_archive_smoke.json
```

该命令默认读取 XUE-LAB Native L2，并同时验收 baseline、delta、trade
物化计数。需要对隔离切片做诊断时才显式传 `--archive-dir`。

外部执行内核差分：

```bash
conda run --no-capture-output -n polyBacktest \
  python scripts/offline_book_differential_worker.py \
  --engine pml2 \
  --input runtime_outputs/offline_execution_fidelity/latest-active-pit-unique/book-differential-corpus.json \
  --output runtime_outputs/pml2_validation/book-differential-pml2.json
```

真实自有订单合同对账：

```bash
conda run --no-capture-output -n polyBacktest \
  python scripts/calibrate_fill_only_v3_live_orders.py
```

该验证必须将 `Paper-L2 shadow`、PML2 与真实 CLOB 终态分开统计，并且至少
输出：

```text
可解析订单数
QUOTE/SHARES 订单数
PML2 可评分数与 abstain 原因
any-fill 与 terminal-class 准确率
成交数量、价格、费用误差
MIN_ORDER_SIZE_CONTRACT_CONFLICT 数量和证据 ID
```

只有 31 笔自有订单时，这是协议合同和正样本一致性检查，不是普遍成交率
证明。发布新默认 profile 前，还必须在不重叠日期、market、类别、价格和流动性
分层上交叉验证至少 10,000 笔模拟订单；数据不足的窗口必须显式记为
`DATA_INSUFFICIENT`，不得从分母删除。

当前订单合同验收结果：

```text
真实 Paper-L2/CLOB 订单：31
PML2 可评分：27
abstain：4（历史记录未保存完整 opposite-side levels）
27 笔可评分订单 any-fill：27/27
27 笔可评分订单 terminal class：27/27
成交量绝对误差总和：0.0000034140 shares
min_order_size 合同冲突：4（均有已接受订单 evidence id）

多市场静态 FAK 交叉验证：15,000 orders
日期/窗口：3 / 12
unique markets / market-days：391 / 433
价格：0.011-0.999
覆盖：浅/中/深盘口、稀疏/中等/活跃成交、窄/普通/宽点差
PML2 与 Nautilus 独立 book-walk 控制：
    status mismatches = 0
    quantity mismatches = 0
    price mismatches above 0.000001 = 0
```

万人级结果可重复检查：

```bash
conda run --no-capture-output -n polyBacktest \
  python scripts/validate_pml2_order_contract_cross_validation.py
```

这项 15,000 单门只验证 arrival-time static L2 FAK 逐档撮合；动态 delta、
Maker queue、共享策略容量、GTD/cancel race 仍由各自的事件回放测试验收，不能
拿静态控制结果替代。

## 8. 当前完成边界

已经完成：

```text
独立执行模式及 HTTP API
run-scoped economic residual
YES/NO 镜像容量与 Maker queue
双时钟和确定性 tie policy
多档 Taker walk
QUOTE BUY/SHARES SELL 原生 amount unit
signed maker/taker amount 及价格改善
venue-admission evidence 与 min-order-size 冲突审计
FOK/FAK/GTC/GTD/post-only
venue delay、cancel race、trading modes
trade/delta 去重
event-driven validity、gap/reset/handover/truncated-depth 门禁
point-in-time L2 archive 冷恢复
动态策略 Python API
统一 ExecutionMatch 和 finalizer adapter
结算、CTF、split/merge/redeem、negative-risk、历史费率基础合同
strict/realistic/optimistic profile matrix
```

本轮已经补齐：

```text
既有 placement/ack/fill/cancel shadow/live 证据接入和 hash pin
Maker interval-censored 生存模型、walk-forward/no-leakage gate 与 API
ALL_LEGS_FOK/HEDGE_ON_LEG_FAILURE 多腿协调
XUE-LAB 冷归档 Maker trade/delta 自动物化
VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED 大单警告和可见容量边界
```

`production_ready=false` 当前只保留一个真实证据阻塞：自有订单 holdout 只有
4 条且全部 `NO_FILL`，缺乏 fill/no-fill 标签多样性，不能据此验证 Maker 概率
迁移。该阻塞不影响本地 L2 Taker/Maker 研究、逐笔审计和多腿执行测试，但在
积累足够的真实自有订单标签前，不能把 Maker 概率称为生产校准概率。

本模型明确不包含 PMXT archive materializer、Native/PMXT event-hash 对账或
策略改变外部市场后的冲击模拟；XUE-LAB Native L2 是唯一历史盘口权威。
FAK/IOC 对大单只成交真实可见容量并取消剩余，FOK 深度不足时整单失败；超出
可见容量的市场反应只记录未建模警告，不用任意倍数门禁，也不虚构后续盘口。
