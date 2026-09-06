# PnL 验证指导

本文档只描述当前推荐的三种 PnL 计算/验证口径：

```text
1. Polymarket 官方 PnL 接口
2. Polymarket Data API 状态口径计算
3. 本地纯链上现金流计算
```

交叉验证目标：

```text
official / data_api / onchain 三者两两误差 < 0.2%
```

## 1. 官方 PnL 接口

### 1.1 接口是什么

当前本地 benchmark 脚本使用的官方 PnL 来源是 Polymarket leaderboard all-time PnL。

主接口：

```text
GET https://data-api.polymarket.com/v1/leaderboard
```

按地址查询时使用参数：

```text
timePeriod=ALL
orderBy=PNL
category=OVERALL
user=<proxy_wallet_address>
limit=25
offset=0
```

返回中用于验证的字段：

```text
pnl     官方 all-time PnL
vol     官方 all-time volume
rank    排名
userName
proxyWallet
```

本地脚本还保留了 fallback 接口：

```text
GET https://lb-api.polymarket.com/profit?address=<proxy_wallet_address>
```

fallback 返回里 `amount` 对应 PnL，`vol` 对应 volume。实际验证优先使用 `/v1/leaderboard`。

### 1.2 直接 curl 示例

```bash
curl 'https://data-api.polymarket.com/v1/leaderboard?timePeriod=ALL&orderBy=PNL&category=OVERALL&user=0xYOUR_ADDRESS&limit=25&offset=0'
```

### 1.3 本地脚本

脚本：

```text
pnl/leaderboard_pnl_benchmark.py
```

单地址运行：

```bash
python pnl/leaderboard_pnl_benchmark.py \
  --addresses 0xYOUR_ADDRESS \
  --leaderboard-limit 1 \
  --max-pages 0 \
  --positions-max-pages 0 \
  --closed-positions-max-pages 0 \
  --run-name official_pnl_check \
  --quiet
```

说明：

```text
官方接口法不是自己重建现金流，而是读取 Polymarket 对该地址聚合后的 benchmark PnL。
它适合作为最终校准标准，但不能解释差异来自哪一笔交易或哪一个市场。
```

## 2. Data API 计算 PnL

### 2.1 推荐使用的 Data API 口径

Data API 有两类数据都可以拿到，但三方验证时只能把状态口径作为默认 PnL：

```text
推荐用于三方验证：
  /positions
  /closed-positions

只用于明细诊断：
  /activity
```

原因是 `/activity` 是公开活动明细，不是一个在所有历史地址上都严格完整的会计账本。之前遇到过地址 `TRADE` activity 行数明显少于本地链上交易，导致 activity 现金流公式偏离官方 PnL；但 `/positions + /closed-positions` 状态口径仍然能和官方 leaderboard 对齐。

### 2.2 官方对齐的 Data API 公式

三方验证时使用这个公式：

```text
Data API state PnL =
  Σ closed_positions.realizedPnl
  + Σ positions.cashPnl
  + Σ positions.realizedPnl
```

字段来源：

```text
closed_positions.realizedPnl  已关闭仓位的已实现 PnL
positions.cashPnl             当前仍在 positions 中的现金 PnL
positions.realizedPnl         当前仍在 positions 中的已实现 PnL
```

这个值在本地脚本里叫：

```text
position_state_pnl.trading_pnl
```

### 2.3 activity 现金流公式

`/activity` 仍然有价值，但它是诊断口径，不是默认验证口径：

```text
Activity cashflow PnL =
  ΣSELL
  + ΣREDEEM
  + ΣMERGE
  + ΣMAKER_REBATE
  - ΣBUY
  - ΣSPLIT
  + unrealized_position_value
```

字段来源：

```text
BUY / SELL       /activity?type=TRADE
REDEEM           /activity?type=REDEEM
MERGE            /activity?type=MERGE
SPLIT            /activity?type=SPLIT
MAKER_REBATE     /activity?type=MAKER_REBATE
unrealized       /positions.currentValue；没有 currentValue 时用 size * curPrice
```

不计入 trading PnL 的外部收入：

```text
REWARD
REFERRAL_REWARD
CONVERSION
```

这些可以单独作为 external income 输出；除非你明确要算 inclusive income，否则不要混进 trading PnL。

### 2.4 Data API 取数方法

用到的接口：

```text
GET https://data-api.polymarket.com/activity
GET https://data-api.polymarket.com/positions
GET https://data-api.polymarket.com/closed-positions
GET https://data-api.polymarket.com/v1/leaderboard
```

activity 按类型拉：

```text
TRADE
REDEEM
MERGE
SPLIT
MAKER_REBATE
REWARD
REFERRAL_REWARD
CONVERSION
```

分页规则在本地脚本中已经处理：

```text
TRADE 使用 timestamp/end 向历史翻页，并额外处理同一秒边界。
非 TRADE activity 使用 offset + sortDirection=ASC 翻页。
positions 使用 user + sizeThreshold=0 + limit + offset。
closed-positions 使用 user + limit + offset。
```

运行时必须使用：

```text
--max-pages 0
--positions-max-pages 0
--closed-positions-max-pages 0
```

含义是拉全量分页。不要用有限页数做最终结论。

### 2.5 Data API 之前踩过的坑

不要把 activity 公式当作所有地址的官方对齐结果。

典型错误：

```text
错误：用 /activity TRADE + REDEEM/MERGE/SPLIT 直接和 leaderboard 做强一致验证。
原因：部分地址 public activity 可能缺 TRADE，或者 closed positions 没有对应 REDEEM activity。
正确：三方验证默认比较 position_state_pnl.trading_pnl。
```

不要按 txHash 去重 TRADE。

```text
一笔 tx 可能有多条真实 fill。
Data API /activity 里同一个 txHash 出现多行时，要逐行计入。
保留第一条会低估 BUY/SELL。
```

不要把 REWARD / REFERRAL_REWARD / CONVERSION 混入 trading PnL。

```text
它们是平台奖励或外部收入，不是交易现金流。
脚本会输出 inclusive_pnl，但三方验证使用 trading_pnl。
```

不要用截图的 1D / 7D / 30D PnL 去验证 all-time 现金流。

```text
本方法计算 all-time。
短周期 PnL 需要周期起点的仓位价值基线，否则不能严格比较。
```

出现以下标记时，activity 明细 PnL 只能用于排查，不应该作为最终结论：

```text
activity_trade_volume_mismatch
closed_positions_without_redeem_activity
activity_cashflow_closed_realized_mismatch
activity_conditions_missing_position_state
legacy_activity_incomplete
positions_truncated
closed_positions_truncated
```

### 2.6 本地脚本

脚本：

```text
pnl/cashflow_pnl_parquet.py
```

运行：

```bash
python pnl/cashflow_pnl_parquet.py \
  --address 0xYOUR_ADDRESS \
  --max-pages 0 \
  --positions-max-pages 0 \
  --closed-positions-max-pages 0 \
  --run-name data_api_pnl_check \
  --quiet
```

重点看输出：

```text
recommended_pnl.source
recommended_pnl.pnl
position_state_pnl.trading_pnl
pnl.trading_pnl
cashflows.by_type.TRADE.total_usdc
leaderboard_aligned_pnl.pnl
completeness.complete
completeness.quality_flags
```

判断规则：

```text
三方验证使用 recommended_pnl.pnl。
正常情况下 recommended_pnl.source = data_api_positions_closed_positions。
如果 positions / closed-positions 分页截断，脚本会退到 data_api_leaderboard，但这类样本不适合证明 Data API 状态公式。
```

## 3. 本地数据库纯链上计算 PnL

### 3.1 当前推荐口径

当前推荐的纯链上计算不是运行时再去 RPC 拉 receipt，而是只使用本地数据库中已经落库的链上事实：

```text
BUY / SELL:
  trades_v2 中的 OrderFilled
  经过 tx_hash + address 分组、settlement leg 过滤后得到

REDEEM / MERGE / SPLIT / MAKER_REBATE:
  non_trade_cashflows
```

运行时保证：

```text
不调用 RPC
不拉 transaction receipt
不调用 Polymarket Data API
只读本地数据库
```

对应脚本：

```text
pnl/db_only_onchain_pnl.py
```

### 3.2 计算公式

本地数据库纯链上现金 PnL：

```text
DB on-chain cash PnL =
  ΣSELL
  + ΣREDEEM
  + ΣMERGE
  + ΣMAKER_REBATE
  - ΣBUY
  - ΣSPLIT
```

如果地址当前仍有持仓，要和官方 all-time PnL 比较，需要额外加当前仓位价值：

```text
DB on-chain PnL for comparison =
  DB on-chain cash PnL
  + unrealized_position_value
```

`db_only_onchain_pnl.py` 本身只计算本地现金流，不主动调用 Data API 获取当前仓位价值。三方验证时，可以从 Data API `/positions` 取 `unrealized_position_value` 后再比较。

### 3.3 BUY / SELL 的 OrderFilled 筛选规则

不能朴素逐行累加 `trades_v2`。正确规则是：

```text
1. 先从 trades_v2 找出该地址作为 maker 或 taker 参与过的 tx_hash。
2. 对这些 tx_hash 拉出同一 tx 内全部 OrderFilled 行。
3. 对每条 OrderFilled 推断 maker 方向：
   - maker_side = BUY  时，maker 是 BUY，taker 是 SELL
   - maker_side = SELL 时，maker 是 SELL，taker 是 BUY
4. 将每条 OrderFilled 投影成 maker/taker 两条候选现金流 leg。
5. 按 tx_hash + address 分组。
6. 如果同一 tx/address 存在 exchange / adapter settlement leg：
   - 只保留 settlement leg
   - 丢弃内部撮合 leg / internal matching leg
7. 如果没有 settlement leg，才保留该 tx/address 的普通 leg。
8. 对目标地址汇总：
   - signed < 0 计入 BUY = abs(signed)
   - signed > 0 计入 SELL = signed
```

settlement leg 的识别依赖本地脚本中的交易对手地址集合：

```text
TRADE_COUNTERPARTY_ADDRESSES
```

这个集合来自：

```text
scripts/trade/orderfilled_erc20_compact_backfill.py
```

在 `db_only_onchain_pnl.py` 中复用。它用于识别 exchange / adapter / settlement 合约地址，避免把同一笔交易里的内部腿重复计入用户 BUY/SELL。

### 3.4 OrderFilled fee / FeeRefunded 规则

`db_only_onchain_pnl.py` 提供参数：

```text
--orderfilled-fee-mode gross
--orderfilled-fee-mode charged
--orderfilled-fee-mode refunded
```

三种模式含义：

```text
gross:
  使用 settlement-leg 过滤后的 OrderFilled 成交金额。
  不扣 OrderFilled.fee。
  不加 FeeRefunded。
  当前用于对齐 Data API /activity TRADE 现金流最稳定。

charged:
  在 maker-side SELL 的场景下，从 SELL proceeds 中扣 OrderFilled.fee。
  不加 FeeRefunded。
  只用于诊断 fee 影响，不作为默认三方验证口径。

refunded:
  先按 charged 扣 OrderFilled.fee。
  再尝试读取本地 orderfilled_fee_refunds 表，把 FeeRefunded 加回。
  如果本地没有 orderfilled_fee_refunds 表，会输出 fee_refund_table_missing。
```

当前推荐默认值：

```text
--orderfilled-fee-mode gross
```

原因：

```text
1. 早期历史区间大多数地址没有 fee-risk。
2. 在 fee-risk 样本中，OrderFilled gross 和 Data API TRADE 的差异通常很小。
3. OrderFilled.fee 不等于用户最终净 fee；直接扣 fee 反而可能扩大误差。
4. 要精确处理 fee，需要额外捕获 FeeRefunded 或等价退款事件。
```

如果以后要把 `refunded` 作为正式口径，需要先落库：

```text
orderfilled_fee_refunds
```

最低字段建议：

```text
tx_hash
to_address / address
refund_raw
block_number
log_index
collateral_token
```

### 3.5 non-trade 现金流来源

非交易现金流来源：

```text
non_trade_cashflows
```

对应 PnL 类型：

```text
REDEEM
MERGE
SPLIT
MAKER_REBATE
```

典型链上来源：

```text
REDEEM        ConditionalTokens PayoutRedemption / adapter redeem / passthrough mint redeem
MERGE         ConditionalTokens PositionsMerge / adapter merge
SPLIT         ConditionalTokens PositionSplit / adapter split
MAKER_REBATE  pUSD / USDC.e rebate distributor transfer
```

这里最容易出问题的是地址归因：

```text
adapter redeem / passthrough mint / wrapped collateral 场景里，
链上会出现多层内部转账。
non_trade_cashflows.py 必须把现金流归因到真实用户地址，
不能归因到 adapter、exchange、CTF 等内部合约地址。
```

### 3.6 生成 REDEEM / MERGE / SPLIT / MAKER_REBATE

脚本：

```text
scripts/trade/non_trade_cashflows.py
```

历史 repair 后继续回补的监督脚本：

```text
scripts/trade/non_trade_repair_then_backfill.py
```

推荐运行方式：

```bash
python -u scripts/trade/non_trade_repair_then_backfill.py \
  --backend mysql \
  --mysql-host 127.0.0.1 \
  --mysql-port 43306 \
  --mysql-user poly_user \
  --mysql-password "$MYSQL_PASSWORD" \
  --mysql-database poly_data \
  --repair-from-block 73058686 \
  --repair-to-block 81576185 \
  --repair-resume-after-synced-at "2026-05-28 10:00:00" \
  --backfill-from-block 81581686 \
  --backfill-to-block 84902319 \
  --batch 500 \
  --fallback-batches 250,100,50 \
  --repair-parallel-workers 2 \
  --backfill-parallel-workers 1 \
  --min-batch 50 \
  --retry-attempts 6 \
  --confirmations 20 \
  --orderfilled-filter-source local \
  --skip-db-count \
  --skip-raw-json \
  --window-delay 0.1 \
  --fail-on-error-window
```

输出表：

```text
non_trade_cashflows
```

sync 窗口表：

```text
non_trade_sync_windows
```

### 3.7 计算本地数据库纯链上 PnL

脚本：

```text
pnl/db_only_onchain_pnl.py
```

单地址运行：

```bash
python pnl/db_only_onchain_pnl.py \
  --backend mysql \
  --mysql-host 127.0.0.1 \
  --mysql-port 43306 \
  --mysql-user poly_user \
  --mysql-password "$MYSQL_PASSWORD" \
  --mysql-database poly_data \
  --trade-source orderfilled \
  --orderfilled-fee-mode gross \
  --address 0xYOUR_ADDRESS \
  --output-json pnl/out/db_only_onchain_pnl.json
```

重点看输出：

```text
orderfilled_pnl.cash_pnl_without_unrealized
orderfilled_pnl.buy_usdc
orderfilled_pnl.sell_usdc
orderfilled_pnl.redeem_usdc
orderfilled_pnl.merge_usdc
orderfilled_pnl.split_usdc
orderfilled_pnl.maker_rebate_usdc
orderfilled_trade.fee_mode
orderfilled_trade.fee_risk_txs
orderfilled_trade.settlement_filtered_groups
orderfilled_trade.internal_legs_dropped
coverage.trade_to_block
coverage.non_trade_to_block
```

按明确水位运行：

```bash
python pnl/db_only_onchain_pnl.py \
  --backend mysql \
  --mysql-host 127.0.0.1 \
  --mysql-port 43306 \
  --mysql-user poly_user \
  --mysql-password "$MYSQL_PASSWORD" \
  --mysql-database poly_data \
  --trade-source orderfilled \
  --orderfilled-fee-mode gross \
  --trade-to-block <orderfilled_safe_block> \
  --non-trade-to-block <non_trade_safe_block> \
  --address 0xYOUR_ADDRESS \
  --output-json pnl/out/db_only_onchain_pnl_cutoff.json
```

注意：`non_trade_sync_windows MAX(to_block)` 不一定等于当前连续 repair 水位。表里可能存在定点 repair 或高区块修复数据，因此验证时要用当前连续 repair 日志里的有效水位，或者显式检查该地址所有 PnL 相关 non-trade cashflow 是否已覆盖。

### 3.8 本地数据库纯链上之前踩过的坑

不要朴素逐行加 `OrderFilled`。

```text
OrderFilled 是成交事实层。
同一 tx 里可能有多条 OrderFilled、内部撮合腿、exchange/adapter settlement leg。
正确做法是 tx_hash + address 分组，并优先保留 settlement leg。
```

不要把 `last_trade_block <= non_trade_waterline` 当作唯一样本条件。

```text
有些地址最后一笔交易很早，但后面很晚才 REDEEM。
例如某地址最后 trade 在 2025 年，但 2026 年仍可能发生 REDEEM。
三方验证 all-time PnL 时，要确认该地址最后一笔 PnL 相关 non-trade cashflow 也已覆盖。
```

不要把表内最大区块当作连续水位。

```text
non_trade_cashflows 可能同时包含：
  - 当前连续 repair 水位以内的完整区间
  - 历史任务写入的数据
  - 定点修复写入的高区块数据

MAX(block_number) 或 non_trade_sync_windows 的高水位，
不一定代表中间所有区间都已经用新逻辑完整 repair。
```

不要默认 `FeeRefunded` 已经纳入。

```text
gross 模式不处理 FeeRefunded。
charged 模式扣 OrderFilled.fee 但不加 FeeRefunded。
refunded 模式需要本地 orderfilled_fee_refunds 表存在。
如果该表不存在，refunded 会标记 fee_refund_table_missing。
```

## 4. 三方交叉验证

### 4.1 三种方法

三方验证要同时比较：

```text
1. 官方 PnL API:
   /v1/leaderboard 的 pnl 字段

2. Data API 状态口径:
   closed_positions.realizedPnl
   + positions.cashPnl
   + positions.realizedPnl

3. 本地数据库纯链上口径:
   db_only_onchain_pnl.py
   --trade-source orderfilled
   --orderfilled-fee-mode gross
   + non_trade_cashflows
```

### 4.2 推荐验证流程

对每个候选地址：

```text
1. 查询官方 /v1/leaderboard PnL。
2. 拉取 Data API /positions 和 /closed-positions，计算 position_state_pnl.trading_pnl。
3. 运行 db_only_onchain_pnl.py，用本地 OrderFilled + non_trade_cashflows 计算 cash PnL。
4. 如果地址有 open positions，加 Data API /positions 的 currentValue。
5. 计算 official_vs_data_api / official_vs_db_onchain / data_api_vs_db_onchain。
```

本地数据库纯链上运行示例：

```bash
python pnl/db_only_onchain_pnl.py \
  --backend mysql \
  --mysql-host 127.0.0.1 \
  --mysql-port 43306 \
  --mysql-user poly_user \
  --mysql-password "$MYSQL_PASSWORD" \
  --mysql-database poly_data \
  --trade-source orderfilled \
  --orderfilled-fee-mode gross \
  --trade-to-block <safe_trade_block> \
  --non-trade-to-block <safe_non_trade_block> \
  --address 0xADDRESS_1 \
  --address 0xADDRESS_2 \
  --output-json pnl/out/db_onchain_validation.json
```

官方/Data API 运行示例：

```bash
python pnl/cashflow_pnl_parquet.py \
  --address 0xYOUR_ADDRESS \
  --max-pages 0 \
  --positions-max-pages 0 \
  --closed-positions-max-pages 0 \
  --run-name data_api_pnl_check \
  --quiet
```

旧脚本说明：

```text
pnl/validate_onchain_pnl.py 和 pnl/pnlOnchain.py 仍然保留了 ERC20 receipt net 验证能力。
但当前推荐的“本地数据库纯链上”验证，不再依赖运行时 receipt 拉取。
新的默认本地验证脚本是 pnl/db_only_onchain_pnl.py。
```

### 4.3 误差公式

本地验证脚本使用：

```text
delta = left - right
denominator = max(1, abs(left), abs(right))
relative_error = abs(delta) / denominator
relative_error_pct = relative_error * 100
```

通过条件：

```text
status = pass
max_relative_error_pct <= 0.2
positions_fetch.truncated = false
closed_positions_fetch.truncated = false
official leaderboard row 存在
```

### 4.4 判断失败来自哪里

先看 official vs Data API：

```text
如果 official_vs_data_api > 0.2%，优先排查 Data API 状态取数：
  - positions 是否 truncated
  - closed_positions 是否 truncated
  - 是否没有拉全 pages
  - leaderboard user 是否对应同一个 proxy wallet
```

再看 official/Data API vs onchain：

```text
如果 official ≈ data_api，但 onchain 偏离：
  - db_only_onchain_pnl.py 是否使用 --trade-source orderfilled
  - OrderFilled 是否已覆盖该地址全部交易区间
  - non_trade_cashflows 是否覆盖该地址最后结算区间
  - non_trade 是否缺 adapter redeem / merge / split
  - 是否把连续 repair 水位和表内零散高区块数据混淆
  - fee-risk 地址是否需要 FeeRefunded 表
  - 当前是否有 open positions，需要加 current value
```

再看 Data API activity 明细：

```text
如果 activity cashflow PnL 偏离，但 position_state_pnl 对齐 official：
  - 这通常不是最终 PnL 错误
  - 说明 /activity 明细不适合作为该地址的严格会计账本
  - 使用 condition_coverage / quality_flags 定位缺失类型
```

### 4.5 推荐样本条件

适合做三方验证的地址：

```text
1. 地址最后一笔交易 block 已被 orderfilled 回补覆盖。
2. 地址后续 REDEEM / MERGE / SPLIT / MAKER_REBATE 已被 non_trade 回补覆盖。
3. Data API /positions 和 /closed-positions 全量分页完成。
4. official 和 Data API position_state_pnl 本身已经对齐。
5. 当前没有未估值 open position；如果有，则 current_value 能明确加入。
6. fee_risk_txs = 0，或已经明确选择 gross/charged/refunded 口径并接受其含义。
```

不适合的地址：

```text
1. Data API positions / closed-positions 分页截断。
2. 本地 non_trade 回补还没覆盖到地址后续结算时间。
3. 有大量仓位迁移或转移，但当前链上归因逻辑还未覆盖。
4. 当前 open positions 价值无法可靠取得。
5. official leaderboard 查不到该地址。
6. 只满足 last_trade_block 早，但后续还有更晚 REDEEM/MERGE/SPLIT。
```

## 5. 最终结论

当前标准口径：

```text
官方 PnL：
  /v1/leaderboard 的 pnl 字段。

Data API PnL：
  closed_positions.realizedPnl
  + positions.cashPnl
  + positions.realizedPnl。

纯链上 PnL：
  本地 trades_v2 OrderFilled
  经 tx_hash + address 分组、settlement leg 过滤得到 BUY/SELL
  + non_trade_cashflows REDEEM/MERGE/MAKER_REBATE
  - non_trade_cashflows SPLIT
  + 必要时加入 current position value。
```

最重要的约束：

```text
OrderFilled 不能朴素逐行累加。
必须使用 settlement leg 过滤后的 tx/address 聚合口径。
默认 fee 模式使用 gross；FeeRefunded 只有在本地 orderfilled_fee_refunds 表存在时才能进入 refunded 口径。
Data API 三方验证默认使用 positions / closed-positions 状态口径，不用 activity 明细口径做最终结论。
```
