# Fill-only V3 中央路由 API

Fill-only V3 只读取 `trade_prints_one_sided` 和市场元数据，不读取 LOB、snapshot、depth 或 queue。

## 真实订单概率校准

历史 Paper-Live 记录可以通过下面的入口，与订单实际到达时刻的 V3 概率逐笔配对；
同一入口还会在输入合同兼容时运行当前 PML2 `realistic`，输出同单对比：

```bash
conda run -n polyBacktest python scripts/calibrate_fill_only_v3_live_orders.py
```

校准使用专用 profile `taker_arrival_probability_only`。它只读取订单到达前的
`OrderFilled` 特征，不运行 future source-confirmed 路径，因此真实订单自身产生的
后续成交不会反向证明自己可成交。FOK 深度拒单是有效负样本；余额、授权、风控等
提交拒绝不作为 `NO_FILL`。Paper 模拟结果用于机械一致性和样本选择审计，真实终态
才是概率标签。

当前 Paper-Live 样本由既有探针策略选择且没有记录 submission propensity，所以报告
只能检验该探针策略覆盖域内的一致性，不能单独证明全市场的实盘成交概率。
旧 V2 profile 和 matcher 保持不变；V3 是有版本号的新研究路径。

## 默认路由

```text
central_trade_only_l2_reference_expected_fak
```

路由规则：

```text
TAKER:
    先尝试 taker_source_confirmed
    有真实 post-arrival source trade -> A_SOURCE_CONFIRMED
    否则 -> 到达前 OrderFilled 特征的分层概率与条件成交量

PASSIVE:
    先尝试 maker_trade_through_lower
    无严格击穿 -> maker_touch_survival_expected

AUTO_BOUND:
    返回分离的 lower / expected / upper scenarios
```

中央路由不会把 modeled expectation 写成 observed fill。每个订单返回 `selected_route`、
`evidence_tier`、`calibration_status`、层级先验 cell 和 price-buffer resolution。

## 合同感知自适应路由

2026-09-03 新增的本地研究 profile：

```text
central_trade_only_contract_aware_adaptive
```

它不使用 LOB，运行时仍只读取订单到达前的 OrderFilled tape。它会按订单
合同选择不同 artifact：

```text
FAK -> FAK_ANY_FILL adaptive v22
FOK -> FOK_FULL_FILL adaptive v14
```

两个 artifact 均绑定 SHA-256；文件与 profile 不一致时请求直接失败。旧
`central_trade_only_contract_aware` 保留用于复现，API 默认值也不会被静默改写。

最小选择方式：

```json
{
  "profile": "central_trade_only_contract_aware_adaptive",
  "orders": []
}
```

该 profile 的 FAK 与 FOK 外部离线验收分别使用 17,100 和 13,800 笔订单。质量门
以同合同 climatology 为参考，使用 market-day 聚类 bootstrap 置信区间，不再
使用跨样本固定的 `Brier <= 0.20` 或 `0.85 <= E/O <= 1.15`。它已通过离线研究门，
但不代表真实下单概率已完成实盘校准。

## TIF-aware 即时单研究路由

需要研究 Polymarket `FAK/FOK/IOC`、但又不想把 TradeTick 伪造成合成盘口时，使用：

```text
central_trade_only_tif_aware_5s
```

预注册的概率门槛只有两档：

```text
central_trade_only_tif_aware_5s         5 秒 p_fill >= 10%，中央研究档
central_trade_only_tif_aware_5s_recall  5 秒 p_fill >= 5%，宽松 sensitivity
```

API 不接受任意 probability multiplier，避免看完策略 PnL 后继续调门槛。

执行顺序：

```text
先在订单真实的一秒有效窗口内尝试 source-confirmed
    -> 有真实 OrderFilled 证据：A_SOURCE_CONFIRMED
    -> 没有：用到达前 OrderFilled 特征计算 MODELED_EXPECTATION
```

概率模型独立训练在 5 秒标签上；对即时 TIF 使用常数 hazard 将 5 秒概率缩放到一秒，
不会把 FAK 偷换成等待 5 秒。没有本地 pre-arrival trade 时，只有分层先验 cell 至少有
100 个训练样本才允许 fallback。先验包含 category、league、price、TTE、liquidity regime
和 BUY/SELL side；不满足支持度就继续 `NO_FILL`。`unknown` 不会被当成一个可用的细分市场，
市场分类缺失时只能回退到 global prior，避免元数据故障抬高成交概率。

`FAK/IOC` 可以报告部分成交的期望量。`FOK` 的场景结果仍只有 0 或整单，响应中的小数是
`P(full fill) * order size` 的期望值，不是一笔历史 partial fill。所有 fallback 都保持：

```text
status = MODELED_EXPECTATION
source_trade_ids = []
expected_fill_is_observed_execution = false
```

因此该 profile 用来做本地研究中央估计，不能并入 source-confirmed observed PnL。

## API

```text
GET  /quant/fill-only/v3/profiles
GET  /quant/fill-only/v3/readiness
POST /quant/fill-only/v3/replay
POST /quant/fill-only/v3/resolve-anchor
```

最小请求：

```json
{
  "requestId": "strategy-run-001",
  "profile": "central_trade_only_tif_aware_5s",
  "sourceMaxBlock": 91652253,
  "defaultLookbackBlocks": 100,
  "defaultHorizonBlocks": 100,
  "orders": [
    {
      "orderId": "order-001",
      "marketId": 3841892,
      "assetId": "TOKEN_ID",
      "side": "BUY",
      "limitPrice": "0.57",
      "size": "10",
      "signalBlock": 91598661,
      "signalTs": "2026-08-01T00:00:00Z",
      "tif": "FAK",
      "liquidityIntent": "TAKER"
    }
  ]
}
```

订单也可显式传 `category`、`league`、`marketSlug`、`marketTitle`、`marketEndTs`。
缺失时服务尝试从 ClickHouse 市场元数据补齐；仍缺失则按先验树回退到较粗 cell 或 global。
未知 JSON 字段返回 HTTP 400。

只有 `signalTs` 时，可以先调用 `resolve-anchor`，也可以在 replay order 中省略
`signalBlock` 让服务按同一份 pinned OrderFilled tape 自动解析。coverage 不完整或锚点不可解析
时返回 HTTP 409，不能改写成 `NO_FILL`。

长周期 Parquet runner、DynamicStrategyReplay、Rust backend 和性能验收命令见
[统一 Fill-only V2/V3 流式回测与 Rust 加速](./统一Fill-only-V2-V3流式回测与Rust加速.md)。

## 校准状态

- 30 秒：独立训练，`READY_SOURCE_CONFIRMED`，中央研究默认。
- 5 秒：29,982 个标签样本，AUC `0.6981`、Brier `0.0333`；用于 TIF-aware
  source-proxy 模型。阈值 precision 只有 `9.80%`，所以仍标记 transfer-unvalidated，
  不能宣称为真实下单成交概率。
- 120 秒：独立训练，`READY_SOURCE_CONFIRMED`，敏感性路径。
- 300 秒：独立训练但当前为 `REVIEW`，不能作为默认主结果。
- 层级先验：18 个真实市场，1/5/30/120/300 秒每个 horizon 35,964 个标签样本；
  TIF-aware artifact 额外按 BUY/SELL side 分层。
- price buffer：只有满足样本量、市场数和 coverage 门槛的 toolkit markout profile 才会激活；否则回退 `0.005`。
- live transfer：必须同时拥有真实 submitted-order fill 与 NO_FILL 标签；当前 readiness 会 fail-close，不会把 OrderFilled proxy 宣称为真实订单概率。

当前推荐的本地研究档为：

```text
central_trade_only_l2_reference_expected_fak
probability model = fill_only_v3_l2_reference_arrival_pit_aligned_grid_v2
```

该模型使用 PML2 作为离线可执行性标签，但运行时只读取订单到达前的 OrderFilled tape。
source-confirmed 路径优先，概率路径只生成 `MODELED_EXPECTATION`。2026-09-01 的三组
保存标签验证如下；主验收与广度复核都超过 10,000 单，补充样本专门检查高活跃分层。

```text
Aug 06-08 primary:
    12,276 orders / 3 dates / 160 markets / 163 market-days
    PML2 order ratio       100.79%
    PML2 quantity ratio    100.86%
    Nautilus quantity      100.43%
    Brier regret             +0.0056

Jul 21-24 broad cross-check:
    11,250 orders / 3 dates / 339 markets / 381 market-days
    PML2 order ratio        98.06%
    PML2 quantity ratio     99.18%
    Nautilus quantity       98.64%
    Brier                    0.1974

Aug 07-08 active-regime supplement:
    11,160 orders / 133 markets / 146 market-days
    PML2 order ratio        96.91%
    PML2 quantity ratio     98.36%
    Nautilus quantity       97.86%
    activity-regime coverage gates 3/3
```

`MODELED_EXPECTATION` 数量不能解释为确定成交订单数。非零期望表示模型分配了一个成交概率
和条件成交量；source-confirmed 下界与 modeled residual 会分账保存。三组验证的窗口、类别、
价格桶和活跃度覆盖通过率均不低于 80%，但仍保留 `PASS_WITH_CALIBRATION_WARNINGS`，因为
个别类别或价格桶存在局部偏差。完整结果位于
`backtest_framework/nautilus_trader_comparison/fill_only_v3_aligned_grid_external_validation_20260901/`。

## 统一主回测入口

V3 也可以通过与 V2/PML2 相同的策略回测入口执行：

```http
POST /quant/backtest-runs
```

关键参数：

```json
{
  "executionPriceMode": "ORDERFILLED_V3_TRADE",
  "executionProfile": "central_trade_only_l2_reference_expected_fak"
}
```

`realistic` 会解析为上述中央研究 profile；审计下界使用
`taker_source_confirmed`。主入口仍通过 `RequiredTradeWindow` 和
`TradeSliceLoader` 只读取订单需要的 `trade_prints_one_sided` 区间，不读取 LOB。

2026-09-01 的真实 HTTP 验收：

```text
run 132 / central modeled:
    expected_fill_size = 8.5223457686
    actual_fill_size   = 0
    standard ledger   = 0 rows
    financial status  = N/A_NO_DEPLOYED_CAPITAL

run 134 / source-confirmed:
    expected_fill_size = 0.2635
    actual_fill_size   = 0.2635
    standard ledger   = 1 BUY row
    source_trade_id   = 69bc7cd92799e34871fa5073aac652b8cc6e710df63607cf636bc919852ae8dd
    financial status  = FINANCIAL_PARTIAL
```

run 134 在 `2026-08-25T13:14:50Z` 仍未结算，所以 finalizer 保留 payout
上下界，不输出完整 ROI。其 trade-slice coverage、TIF、fill-probability evidence、
现金账和 execution/ledger parity 检查均为 `ready`。

这两个 canary 共同验证：概率期望不会改变真实资金，只有带 source evidence 的 actual fill
才进入现金账和结算。它们不是概率模型的统计验证；万人级统计质量仍以上面的三个跨窗口结果为准。

校准命令：

```bash
conda run -n polyBots python scripts/calibrate_trade_only_hierarchical_priors.py
conda run -n polyBots python scripts/calibrate_orderfilled_probability.py \
  --horizon-seconds 5 --horizon-blocks 15 \
  --output-profile config/execution/orderfilled_probability_profile.5s.v1.json \
  --output-report runtime_outputs/fill_trade_validation/orderfilled_probability_calibration.5s.json
python scripts/calibrate_toolkit_markout_buffer.py --input toolkit-markout.json
python scripts/import_real_order_state_events.py --input real-order-events.jsonl
python scripts/validate_fill_only_v3_tif_aware.py \
  --cohort validation_a.json --cohort validation_b.json
```

`IOC/FOK/FAK` 仍保持一秒语义。需要 30/120/300 秒窗口时使用 `GTD`，不能通过回测器把 IOC 偷换成长等待订单。

## 真实 Paper-Live 概率校准状态（2026-09-02）

校准入口：

```bash
conda run -n polyBacktest python scripts/calibrate_fill_only_v3_live_orders.py
```

该入口把每笔真实订单在 venue arrival 时刻重放到
`taker_arrival_probability_only`。预测只使用 arrival 之前的 OrderFilled tape，
不会让订单自己的后续成交反过来提高预测概率，也不读取 LOB。

当前数据库共有 35 次真实提交，其中 31 次具备可校准的终态：

```text
FULL       29
PARTIAL     1
REJECT      1
HTTP_REJECTED 4  # 非流动性终态，不作为 NO_FILL 标签
```

相同 run 因市场标题变化可能导出为不同文件名。加载器现在按稳定的
`sample_id/run_id` 去重，并优先保留带 `selection_contract` 的记录，避免同一订单双算。

完成目标区块的 one-sided tape 派生后，结果为：

```text
unique execution-label records     31
scored by V3                       30
coverage abstain                    1  # 2026-09-01 超出当前 raw 派生覆盖
independent markets                18
independent dates                  10
positive labels                    29
negative labels                     1
Paper-L2 shadow matches           31/31
PML2 realistic scored             10/31
PML2 any-fill matches              8/10
PML2 input abstain                21/31

mean predicted probability    0.6647323730
observed positive rate        0.9666666667
Brier score                  0.1390972377
base-rate Brier              0.0322222222
Brier regret                 0.1068750155
fill-fraction MAE            0.3501378632
```

这里必须区分两个 L2 执行器：

```text
paper_taker_l2_v7_shadow_head
    原模拟盘 L2 引擎
    31/31 与真实订单的终态、数量、价格和费用匹配

PREDICTION_L2_REPLAY_V1 / realistic
    当前 PML2 回测引擎
    只对 10 笔 shares 订单且保存了完整对手盘档位的记录具备兼容输入合同
    其中 8/10 的 any-fill 判断正确
```

PML2 的 21 笔 abstain 不能记成 `NO_FILL`：20 笔是 QUOTE-denominated BUY，当前
`Pml2OrderIntent` 只有 shares 数量合同；1 笔历史记录只有 best bid/ask，没有完整对手盘档位。
回放器明确拒绝这些记录，没有把 quote 金额偷偷换算成 shares。

PML2 支持域内的两笔 false negative 都来自 `below_market_min_order_size`：历史快照记录
`min_order_size=5`，但真实 venue 分别接受并成交了 2 股和 1 股。这个结果暴露的是当前
PML2 最小下单量门禁或历史 metadata 语义仍需校准，不应通过删除失败样本提高成绩。

同单结果现在位于报告的 `same_order_model_comparison`，每笔订单的 PML2 结果位于
`orders[].pml2_replay`。原 Paper-L2 的 `31/31` 证明模拟盘对账管线工作正常；它既不等于
PML2 的成绩，也不证明 V3 的绝对成交概率准确。

这批订单全部来自人工 market allowlist 和显式 approved asset，不是从可交易订单总体中
随机抽取。每个导出文件现在都会显式保存：

```json
{
  "selection_contract": {
    "sampling_policy": "MANUAL_MARKET_ALLOWLIST_AND_APPROVED_ASSET",
    "candidate_randomized": false,
    "submission_propensity": null,
    "population_identifiable": false
  }
}
```

因此 readiness 正确返回：

```text
status                              BLOCKED_INSUFFICIENT_REAL_LABELS
ready_for_local_research            true
ready_for_live_transfer_claim       false
population_probability_claim_allowed false
```

默认质量门还需要：

```text
additional scored labels            170
additional real negative labels      19
additional independent markets        2
additional independent dates          0
future rows with logged propensity   required
```

负例必须来自真实提交后的流动性终态。Paper、PML2 或 Nautilus 标签仍可用于离线 proxy
验证和模型开发，但不能替代真实 NO_FILL/REJECT 标签。后续采样必须采用已知概率的随机或
分层随机政策并逐笔记录 propensity；不能继续只挑预计会成交的订单再声称总体成交概率。

当前可认可的范围是：

```text
工程实现和回放合同                     认可
万人级 PML2/Nautilus proxy 交叉验证      认可为本地研究证据
当前真实正例的一致性                    认可
V3 概率排序用于本地策略筛选              可用但需保留警告
V3 输出的绝对概率等于真实下单概率          不认可
使用该概率直接做实盘资金决策              不允许
```

机器可读结果位于：

```text
runtime_outputs/taker_calibration/fill-only-v3-live-calibration-latest.json
```
