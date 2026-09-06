# Fill-only 全市场金融结算

## 范围

这套结算层不属于某一个策略。输入只有两类冻结 artifact：

```text
任意策略的 Fill-only 正持仓 inventory
半年内所有发生过 trade_prints_one_sided 的 market settlement catalog
```

策略名称、信号因子和订单生成方式不参与 payout 判断。同一个 catalog 可以被任意 V2/V3、冻结订单或动态策略回测复用。

“所有 market 都可以结算”的准确含义是：每个 market 都能进入流程并得到唯一、可审计的状态；未公开结果的 market 不会被伪造成亏损。

## Market 状态

```text
SETTLED_VERIFIED_YES_AS_OF_CUTOFF
SETTLED_VERIFIED_NO_AS_OF_CUTOFF
CANCELLED_VERIFIED_HALF_REFUND_AS_OF_CUTOFF
POST_CUTOFF_SETTLEMENT
UNRESOLVED_AS_OF_CUTOFF
TOKEN_OR_IDENTITY_CONFLICT
TECHNICAL_EVIDENCE_MISSING
```

只有前三类产生确定 payout。`POST_CUTOFF`、`UNRESOLVED` 和技术缺失会保留为未完成头寸，同时输出组合 payout/PnL 上下界。

## Point-in-time 证据

目录支持三种显式证据政策：

```text
oracle-finalized
    普通回测的默认政策。
    要求 market/condition/token 映射一致、Oracle 最终结果、结算交易、
    结算区块、可信结算时间，以及冻结 cutoff 边界。

exact-header
    每个 settlement event 都要求独立 exact Polygon block header。
    只用作更强的链上审计敏感性，不是普通 PnL 的必要条件。

oracle-event-cutoff-block
    验证 market/token、Oracle event id、condition、tx hash、block、outcome；
    再用一对相邻 canonical Polygon blocks 冻结 cutoff 边界。
    允许缺少可信 settlement timestamp 的 block-only 结算。
    仅保留用于复现旧结果，不再作为默认财务口径。
```

普通 `oracle-finalized` 结算严格执行：

```text
1. market / condition / token 对应正确
2. Oracle 已给出最终结果
3. 有结算交易和结算区块编号
4. 有可信的结算时间
5. 策略最后成交区块 < 结算区块
6. 结算发生在冻结截止时间之前
```

可信时间优先使用 `oracle.oracle_events.event_time`，其次使用一致的 status event time；两者缺失时，可以使用同一结算区块的 `trade_time:pg_api_trades_tx_path` 或 `trade_time:pg_api_trades_main`。这些来源只能提供普通回测时间证据，不能伪装成 exact canonical block hash。

当前冻结截止时间 `2026-08-25T13:14:50Z` 的相邻边界是：

```text
block 92,639,108  2026-08-25T13:14:49Z
block 92,639,109  2026-08-25T13:14:51Z
```

结算 block 小于等于前者才是 cutoff 前公开结果。若 Oracle event time 与 block 边界冲突，记录会 fail closed。

## 构建半年目录

```bash
conda run -n prediction-market-quant python \
  scripts/build_fill_only_settlement_catalog.py \
  --all-traded-markets \
  --trade-from-ts 2026-02-01T00:00:00Z \
  --trade-to-ts 2026-07-25T00:00:00Z \
  --cutoff-ts 2026-08-25T13:14:50Z \
  --evidence-policy oracle-finalized \
  --partition-size 25000 \
  --market-chunk-size 10000 \
  --block-chunk-size 10000 \
  --resume \
  --output-dir runtime_outputs/fill_only_settlement/all_traded_catalog
```

构建器会冻结 `market_inventory.parquet`、market-id hash、build contract、每个分片 hash 和根 catalog hash。恢复运行只校验并跳过已完成分片，不会重新解析它们。

## 当前半年全市场验收

2026-02-01 至 2026-07-25 的真实 `trade_prints_one_sided` inventory 已完整构建：

```text
market_count                 1,687,187
part_count                          68
catalog_sha256               b59451f2cdb35b279a75c01d6e42ce0235293293e17a7810b9f442ea4c585a41

SETTLED_VERIFIED_YES           406,916
SETTLED_VERIFIED_NO            618,228
CANCELLED_HALF_REFUND            7,841
UNRESOLVED_AS_OF_CUTOFF         45,546
TECHNICAL_EVIDENCE_MISSING     608,656
```

`TECHNICAL_EVIDENCE_MISSING` 中最大一类是 `MARKET_STATUS_NOT_FOUND`。它们主要是只有单边 token 的 `orderfilled-placeholder`、combo 或未完成 Oracle/status 派生的 market。这些 market 有真实成交数据，但当前数据库不足以证明谁赢；目录会保留它们，而不是强制标记为输。

目录位于：

```text
runtime_outputs/fill_only_settlement/
  all_traded_20260201_20260725_asof_20260825_oracle_finalized_v2/
```

相同 build contract 的 `--resume` 复验耗时约 4.2 秒，产出相同 catalog hash。

## 结算任意策略

先将执行结果转换为通用正持仓：

```bash
conda run -n prediction-market-quant python \
  scripts/export_timestamp_native_fill_only_positions.py \
  --execution-root /absolute/path/to/timestamp_native_fill_only_run \
  --profile probabilistic_trade_tape \
  --output-dir runtime_outputs/fill_only_settlement/my_positions
```

再运行金融 finalizer：

```bash
conda run -n prediction-market-quant python \
  scripts/finalize_fill_only_financials.py \
  --execution-positions runtime_outputs/fill_only_settlement/my_positions \
  --settlement-catalog runtime_outputs/fill_only_settlement/all_traded_catalog \
  --required-evidence-grade oracle-finalized \
  --output-dir runtime_outputs/fill_only_settlement/my_financials
```

`oracle-finalized` 是默认值。若要运行额外的 exact-header 审计子集，显式改为：

```text
--required-evidence-grade exact-header
```

旧结果复现才使用：

```text
--required-evidence-grade oracle-event-cutoff-block
```

finalizer 只读取持仓涉及的 catalog 分片，不会把半年全目录载入内存。输出包括 position ledger、cashflow ledger、daily realized equity、完整/部分组合状态、PnL/ROI、结算子集诊断和未结算上下界。

执行库只需输出公共 `FillOnlyExecutionPosition` 合同，它不需要是 PMQ-073 或任何特定策略。旧 timestamp-native 长回测的 adapter 只是兼容入口；新策略可以直接写该 artifact。

## HTTP API

```text
POST /quant/fill-only/settlements/resolve
GET  /quant/fill-only/settlements/<market_id>?cutoffTs=...
GET  /quant/fill-only/settlements/coverage?cutoffTs=...

POST /quant/backtest-runs/<run_id>/finalize
GET  /quant/backtest-runs/<run_id>/financials
GET  /quant/backtest-runs/<run_id>/finalization-status
```

同步 resolve 最多处理 5,000 个 market。半年全量必须使用可恢复的分区构建器；不能把长任务塞进一个 HTTP 请求。

两个 POST 接口都默认 `evidencePolicy=oracle-finalized`，也可显式传入：

```json
{
  "cutoffTs": "2026-08-25T13:14:50Z",
  "evidencePolicy": "oracle-finalized"
}
```

未知字段和未知 evidence policy 返回 HTTP 400，不会静默回退。

## 当前真实长回测复算

同一批 173 天成交结果没有重新撮合，只重跑 settlement catalog 和金融 finalizer：

```text
positions                         181,184
markets                           136,269
verified settled positions       167,539
unresolved positions               7,761
technical positions                5,884

settled subset entry        2,305,929.9118924387 USDC
settled subset payout       2,291,276.2126275099 USDC
settled subset net PnL        -14,653.6992649288 USDC
settled subset ROI                    -0.6354789532%
```

该 settled subset 是当前已能证明结算的主诊断结果，但完整组合仍为 `FINANCIAL_PARTIAL`，因此不能把它称为全部 181,184 个持仓的最终收益。`exact-header` 的 `+1.1158%` 只表示较小 exact-header 覆盖子集，不与普通结果并列作为策略收益。

新 artifact 位于：

```text
runtime_outputs/fill_only_settlement/
  full_v5_primary_catalog_oracle_finalized_v2/
  full_v5_primary_financials_oracle_finalized_v2/
```

## 财务边界

```text
YES winner payout       = filled_size * 1
NO winner payout        = filled_size * 1
losing token payout     = 0
verified cancellation   = filled_size * 0.5
net PnL                 = payout - entry_notional - recorded_fee
```

成交必须早于协议结算。普通模式要求双方都有 block，并以 `last_fill_block < settlement_block` 为主排序证据；秒级时间戳相同不会推翻严格的区块先后关系。缺少可信 settlement timestamp 时普通模式不会计算 payout，只有旧的 `oracle-event-cutoff-block` 复现模式允许 block-only 结算。
