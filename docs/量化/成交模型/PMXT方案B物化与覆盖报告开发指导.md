# PMXT 方案 B：coverage-only 优先，DEPTH 候选再物化

目标：先对本地已有 PMXT raw parquet 做轻量 coverage-only 扫描，只读取 `hour / condition_id / asset_id / event_type / timestamp`，不拆盘口 levels，不写大 event 表。先判断 PMXT 和本地 market / token / OrderFilled 的覆盖率，以及哪些 market 可以做 DEPTH、哪些只能 fill-first。只有被判定为 DEPTH 候选的 market/hour，才进入完整 LOB book snapshot / price_change 物化。

## 1. 开发原则

1. 不新增下载。第一阶段只处理本地 `/data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive`。
2. 覆盖率判断必须先走 coverage-only，不允许为了算覆盖率先全量物化 LOB。
3. coverage-only 只读轻量字段：`source_hour / condition_id / asset_id / event_type / timestamp`。
4. coverage-only 不写 `pmxt_l2_event_compact` / `pmxt_l2_book_level_compact` 这类大表。
5. raw parquet 不能直接删除。只有 DEPTH 候选完成物化、校验、报告后，才允许生成 quarantine 候选。
6. market 覆盖报告只生成总报告，不生成每个 market 一个长报告。
7. 所有结论必须来自实际表数据和校验结果，不能用“应该覆盖”代替。

## 2. 正确开发顺序

第一步必须先跑 coverage-only：

```bash
conda run -n polyBots python experiments/PMXT/build_pmxt_coverage_only_report.py \
  --pmxt-root /data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive \
  --start-hour 2026-03-23T01 \
  --end-hour 2026-03-23T01 \
  --output-dir runtime_outputs/pmxt_coverage_only/smoke_20260323T01
```

coverage-only 输出：

```text
runtime_outputs/pmxt_coverage_only/.../pmxt_coverage_only_report.md
runtime_outputs/pmxt_coverage_only/.../pmxt_coverage_only_report.json
runtime_outputs/pmxt_coverage_only/.../pmxt_coverage_only_market_table.csv
```

只有 coverage-only 报告中 `execution_tier = depth` 的 market/hour，才进入后续 compact LOB 物化。

## 3. compact 物化阶段输出

compact 物化不是覆盖率判断的第一步。它只服务于 DEPTH 候选 market/hour 的后续回测执行。输出：

```text
runtime_outputs/pmxt_materialized_coverage/pmxt_materialized_coverage_report.md
runtime_outputs/pmxt_materialized_coverage/pmxt_materialized_coverage_report.json
runtime_outputs/pmxt_materialized_coverage/pmxt_materialized_coverage_table.csv
```

Markdown 报告结构：

```text
# PMXT Compact L2 覆盖报告

## 总览
- raw parquet 文件数
- raw parquet 总大小
- 已物化文件数
- 物化 L2 行数
- ClickHouse compact 表大小
- 压缩比例
- PMXT L2 覆盖 market 数
- 和本地 market metadata 对齐 market 数
- 和 OrderFilled 对齐 market 数
- 可做 DEPTH 的 market 数
- 只能 fill-first 的 market 数

## market 覆盖表
一张总表，每行一个 market。

## 缺失原因统计
按 no_depth_reason / fill_first_reason 聚合。

## 删除 raw parquet 建议
列出哪些小时已经通过校验，可以进入 quarantine。
```

## 3. 总报告表格字段

总报告里只要一张主表即可，每个 market 一行：

| 字段 | 含义 |
| --- | --- |
| `market_id` | 本地 market id |
| `market_slug` | 本地 market slug |
| `condition_id` | PMXT / CLOB condition id |
| `token_count` | 对齐到的 token 数 |
| `pmxt_l2_start` | PMXT L2 最早事件时间 |
| `pmxt_l2_end` | PMXT L2 最晚事件时间 |
| `orderfilled_start` | OrderFilled 最早成交时间 |
| `orderfilled_end` | OrderFilled 最晚成交时间 |
| `l2_active_hours` | 有 L2 depth event 的小时数 |
| `orderfilled_active_hours` | 有 OrderFilled 的小时数 |
| `fill_count` | OrderFilled 成交数 |
| `fills_with_any_prior_l2_pct` | fill 之前存在任意 L2 的比例 |
| `fresh_l2_5m_pct` | fill 前 5 分钟内有 L2 的比例 |
| `fresh_l2_1h_pct` | fill 前 1 小时内有 L2 的比例 |
| `l2_fill_overlap_hours_pct` | OrderFilled 活跃小时中有 L2 的比例 |
| `can_depth` | 是否可以做 DEPTH execution |
| `execution_tier` | `depth` / `fill_first` / `price_only` / `unusable` |
| `no_depth_reason` | 不能做 DEPTH 的原因 |

示例：

```markdown
| market_id | market_slug | PMXT L2 | OrderFilled | L2 active h | OF active h | 5m fresh | 1h fresh | tier | reason |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 123 | nba-lal-det-2026-03-23 | 2026-03-20 -> 2026-03-23 | 2026-03-22 -> 2026-03-23 | 51 | 9 | 98.2% | 100.0% | depth | ready |
| 456 | some-market | 2026-05-01 -> 2026-05-01 | 2026-05-10 -> 2026-05-11 | 1 | 12 | 0.0% | 0.0% | fill_first | stale_l2 |
```

## 4. Compact 物化表设计

建议新增 ClickHouse 表，而不是复用当前样本表。

### 4.1 `pmxt_l2_event_compact`

只保存回测必要字段：

```sql
CREATE TABLE IF NOT EXISTS poly_orderfilled.pmxt_l2_event_compact
(
    condition_id String CODEC(ZSTD(3)),
    market_id UInt64 CODEC(Delta(8), ZSTD(3)),
    market_slug String CODEC(ZSTD(3)),
    token_id String CODEC(ZSTD(3)),
    token_side LowCardinality(String) CODEC(ZSTD(3)),
    source_hour DateTime CODEC(Delta(4), ZSTD(3)),
    event_time DateTime64(3) CODEC(Delta(8), ZSTD(3)),
    event_type LowCardinality(String) CODEC(ZSTD(3)),
    operation LowCardinality(String) CODEC(ZSTD(3)),
    side LowCardinality(String) CODEC(ZSTD(3)),
    price Nullable(Decimal(20, 10)) CODEC(ZSTD(3)),
    size Nullable(Decimal(30, 10)) CODEC(ZSTD(3)),
    best_bid Nullable(Decimal(20, 10)) CODEC(ZSTD(3)),
    best_ask Nullable(Decimal(20, 10)) CODEC(ZSTD(3)),
    source_row_index UInt64 CODEC(Delta(8), ZSTD(3)),
    source_event_index UInt32 CODEC(Delta(4), ZSTD(3)),
    source_hash String CODEC(ZSTD(3)),
    build_tag LowCardinality(String) DEFAULT 'pmxt_compact_l2_v1',
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(source_hour)
ORDER BY (market_id, token_id, event_time, source_row_index, source_event_index);
```

### 4.2 `pmxt_l2_book_level_compact`

`book` / `book_snapshot` 事件需要能重建盘口，所以 level 单独拆表：

```sql
CREATE TABLE IF NOT EXISTS poly_orderfilled.pmxt_l2_book_level_compact
(
    condition_id String CODEC(ZSTD(3)),
    market_id UInt64 CODEC(Delta(8), ZSTD(3)),
    market_slug String CODEC(ZSTD(3)),
    token_id String CODEC(ZSTD(3)),
    source_hour DateTime CODEC(Delta(4), ZSTD(3)),
    event_time DateTime64(3) CODEC(Delta(8), ZSTD(3)),
    side LowCardinality(String) CODEC(ZSTD(3)),
    level_index UInt16 CODEC(Delta(2), ZSTD(3)),
    price Decimal(20, 10) CODEC(ZSTD(3)),
    size Decimal(30, 10) CODEC(ZSTD(3)),
    source_hash String CODEC(ZSTD(3)),
    build_tag LowCardinality(String) DEFAULT 'pmxt_compact_l2_v1',
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(source_hour)
ORDER BY (market_id, token_id, event_time, side, level_index);
```

### 4.3 `pmxt_l2_hour_inventory`

记录每个 raw parquet 小时是否已物化、是否可删除：

```sql
CREATE TABLE IF NOT EXISTS poly_orderfilled.pmxt_l2_hour_inventory
(
    source_hour DateTime,
    source_file String,
    raw_size_bytes UInt64,
    raw_sha256 String,
    schema_type LowCardinality(String),
    parquet_rows UInt64,
    condition_count UInt64,
    asset_count UInt64,
    event_rows_written UInt64,
    level_rows_written UInt64,
    materialized_status LowCardinality(String),
    delete_status LowCardinality(String),
    warning String,
    build_tag LowCardinality(String) DEFAULT 'pmxt_compact_l2_v1',
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_hour, source_file);
```

## 5. 对齐规则

PMXT 对齐必须按这个顺序做：

```text
condition_id -> 本地 market metadata
asset_id/token_id -> 本地 token metadata
event_time/source_hour -> OrderFilled block timestamp
market_id + token_id + time window -> fill fresh L2 coverage
```

不能只用 market_slug 模糊匹配。

## 6. DEPTH 判定规则

建议第一版使用下面的门槛：

```text
can_depth = true 当且仅当：
  matched_local_market = true
  token_count >= 2
  fill_count > 0
  fresh_l2_5m_pct >= 80%
  fresh_l2_1h_pct >= 95%
  l2_fill_overlap_hours_pct >= 80%
```

否则：

```text
fresh_l2_1h_pct > 0:
    execution_tier = fill_first
    no_depth_reason = partial_l2

没有 L2 但有 OrderFilled:
    execution_tier = fill_first
    no_depth_reason = no_fresh_l2

没有 OrderFilled 但有 L2:
    execution_tier = price_only
    no_depth_reason = no_orderfilled

两者都没有:
    execution_tier = unusable
    no_depth_reason = no_l2_no_orderfilled
```

## 7. 脚本开发顺序

### 第零步：按小时精确对齐，生成候选清单

不能再使用一次性加载全部 OrderFilled 的旧精确对齐脚本。使用：

```bash
conda run -n polyBots python experiments/PMXT/build_pmxt_orderfilled_streaming_alignment.py \
  --start-hour 2026-03-16T14 \
  --end-hour 2026-06-23T00 \
  --output-dir runtime_outputs/pmxt_orderfilled_streaming_alignment/full
```

该脚本按小时执行：

```text
hour cache 筛 condition/token
-> ClickHouse 按小时聚合 canonical OrderFilled
-> raw PMXT 只读相关 pair 的精确 timestamp
-> 计算 fill 前 5m/1h freshness
-> 每小时 Parquet checkpoint
-> 输出 DEPTH / fill-first market 清单
```

主要产物：

```text
pmxt_orderfilled_streaming_market_table.csv
depth_markets.csv
fill_first_markets.csv
depth_market_hours.csv
depth_hours.txt
materialize_depth_candidates.sh
```

只有 `depth_markets.csv` 和 `depth_hours.txt` 可以作为完整盘口物化输入。直接执行生成的 `materialize_depth_candidates.sh` 才会写 ClickHouse；脚本带磁盘余量门禁，不下载、不删除 raw parquet。

### 第一步：只跑 24 小时 smoke

新增脚本：

```text
experiments/PMXT/materialize_pmxt_compact_l2.py
experiments/PMXT/run_pmxt_compact_materialize_backfill.py
experiments/PMXT/build_pmxt_compact_coverage_report.py
experiments/PMXT/check_pmxt_compact_acceptance.py
experiments/PMXT/audit_pmxt_compact_archive.py
experiments/PMXT/check_pmxt_compact_full_acceptance.py
experiments/PMXT/plan_pmxt_raw_delete_candidates.py
```

命令形态：

```bash
conda run -n polyBots python experiments/PMXT/materialize_pmxt_compact_l2.py \
  --pmxt-root /data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive \
  --start-hour 2026-06-22T00 \
  --end-hour 2026-06-22T23 \
  --build-tag pmxt_compact_l2_smoke_20260622 \
  --write

conda run -n polyBots python experiments/PMXT/build_pmxt_compact_coverage_report.py \
  --build-tag pmxt_compact_l2_smoke_20260622 \
  --output-dir runtime_outputs/pmxt_materialized_coverage/smoke_20260622

conda run -n polyBots python experiments/PMXT/check_pmxt_compact_acceptance.py \
  --build-tag pmxt_compact_l2_smoke_20260622 \
  --report-json runtime_outputs/pmxt_materialized_coverage/smoke_20260622/pmxt_materialized_coverage_report.json \
  --min-raw-files 1 \
  --min-compact-event-rows 1 \
  --min-compact-bytes 1 \
  --min-pmxt-l2-markets 1 \
  --require-market-rows

conda run -n polyBots python experiments/PMXT/audit_pmxt_compact_archive.py \
  --pmxt-root /data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive \
  --build-tag pmxt_compact_l2_smoke_20260622 \
  --output-dir runtime_outputs/pmxt_materialized_coverage/smoke_20260622

conda run -n polyBots python experiments/PMXT/check_pmxt_compact_full_acceptance.py \
  --build-tag pmxt_compact_l2_smoke_20260622 \
  --coverage-report-json runtime_outputs/pmxt_materialized_coverage/smoke_20260622/pmxt_materialized_coverage_report.json \
  --archive-audit-json runtime_outputs/pmxt_materialized_coverage/smoke_20260622/pmxt_compact_archive_audit.json \
  --min-compact-event-rows 1 \
  --min-compact-bytes 1 \
  --min-pmxt-l2-markets 1 \
  --min-local-raw-files 1 \
  --min-ready-files 1 \
  --require-market-rows
```

检查：

```text
raw 文件数 = inventory 文件数
parquet rows > 0
event_rows_written > 0
ClickHouse 表大小可查询
coverage report 中 market 行数 > 0
DEPTH / fill_first / price_only 分类有结果
acceptance gate 返回 0
archive audit 输出 local raw 文件数、ready 文件数、missing 文件数、ready raw byte pct
full acceptance gate 返回 0
```

如果只想先验证解析，不写数据库，去掉 `--write`，并加小样本限制：

```bash
conda run -n polyBots python experiments/PMXT/materialize_pmxt_compact_l2.py \
  --pmxt-root /data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive \
  --start-hour 2026-06-23T00 \
  --end-hour 2026-06-23T00 \
  --build-tag pmxt_compact_l2_dry_run_check \
  --batch-size 1000 \
  --max-raw-rows 100
```

### 第二步：跑 7 天验证

确认 24 小时正常后，再跑连续 7 天，观察：

```text
物化耗时
ClickHouse 写入速度
compact/raw 空间比例
fresh L2 覆盖分布
异常小时数量
```

连续多小时不要手工循环调用物化脚本，使用可恢复 runner：

```bash
conda run -n polyBots python experiments/PMXT/run_pmxt_compact_materialize_backfill.py \
  --pmxt-root /data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive \
  --start-hour 2026-06-22T00 \
  --end-hour 2026-06-28T23 \
  --build-tag pmxt_compact_l2_7d_20260622 \
  --max-hours 24 \
  --smallest-first \
  --max-selected-raw-bytes 10737418240 \
  --min-free-gb 500 \
  --sleep-between-hours 5 \
  --write \
  --run-report \
  --run-acceptance \
  --require-market-rows
```

这个 runner 负责：

```text
按本地 raw parquet 小时枚举
跳过 inventory 中已经 ready 的小时
失败小时写入 state json
支持 --plan-only / --max-hours / --force-hour
支持 --smallest-first / --max-selected-raw-bytes，方便先吃小文件批次
支持 --min-free-gb，避免 /data2 空间不足
支持 --sleep-between-hours，避免连续压 ClickHouse
不下载数据
不删除 raw parquet
```

### 第三步：跑本地 955 个 parquet 全量

全量只在前两步通过后执行。

全量执行必须支持：

```text
断点续跑
已完成小时跳过
失败小时记录
重复 build_tag 防重
--dry-run
--max-hours
--force-hour
```

全量前后都要跑 archive audit。全量完成时，`ready_files` 必须等于 `local_raw_files`，`ready_raw_byte_pct` 必须是 `100%`。

全量验收命令必须使用严格门禁：

```bash
conda run -n polyBots python experiments/PMXT/check_pmxt_compact_full_acceptance.py \
  --build-tag pmxt_compact_l2_full \
  --coverage-report-json runtime_outputs/pmxt_materialized_coverage/full/pmxt_materialized_coverage_report.json \
  --archive-audit-json runtime_outputs/pmxt_materialized_coverage/full/pmxt_compact_archive_audit.json \
  --min-compact-event-rows 1 \
  --min-compact-bytes 1 \
  --min-pmxt-l2-markets 1 \
  --min-local-raw-files 955 \
  --min-ready-file-pct 100 \
  --min-ready-raw-byte-pct 100 \
  --max-missing-files 0 \
  --require-archive-complete \
  --require-market-rows
```

## 8. raw parquet 删除策略

第一版脚本不允许直接删除 raw parquet。

允许做：

```text
--mark-delete-candidates
```

把满足下面条件的小时标记为 `delete_candidate`：

```text
materialized_status = ready
raw_sha256 非空
parquet_rows > 0
event_rows_written > 0
coverage report 已生成
抽样盘口可重建
```

下一步再用单独脚本把 raw 移入：

```text
/data2/jiahuaiyu/prediction-market-quant/runtime_outputs/pmxt_archive_quarantine
```

确认一段时间后再删除。

删除候选清单由非破坏性脚本生成：

```bash
conda run -n polyBots python experiments/PMXT/plan_pmxt_raw_delete_candidates.py \
  --build-tag pmxt_compact_l2_7d_20260622 \
  --report-json runtime_outputs/pmxt_materialized_coverage/smoke_20260622/pmxt_materialized_coverage_report.json \
  --output-dir runtime_outputs/pmxt_materialized_coverage/smoke_20260622 \
  --require-report
```

这个脚本只输出：

```text
pmxt_raw_delete_candidates.md
pmxt_raw_delete_candidates.json
pmxt_raw_delete_candidates.csv
```

它不会移动文件，也不会删除文件。

## 9. Codex 执行要求

Codex 开发时必须按下面顺序执行：

1. 先查现有表结构和本地 PMXT 路径，不要猜字段。
2. 第一版只处理本地 parquet，不做远端下载。
3. 先实现 24 小时 smoke，不直接跑全量。
4. 每个脚本都要有 `--dry-run`、`--start-hour`、`--end-hour`、`--build-tag`。
5. 报告必须是一个总报告，market 明细用表格，不输出每个 market 一个长报告。
6. 任何删除 raw 的动作必须拆成单独步骤，并默认关闭。
7. 最终回答必须给出 raw size、compact table size、压缩比例、DEPTH-ready market 数、fill-first market 数。

## 10. 完成标准

24 小时 smoke 完成标准：

```text
pmxt_l2_event_compact 有数据
pmxt_l2_book_level_compact 有数据
pmxt_l2_hour_inventory 有 ready 小时
总覆盖报告生成
报告里 market 表不为空
能查询 compact 表空间大小
能计算 raw -> compact 压缩比例
archive audit 能证明当前 build_tag 覆盖了哪些本地 raw 小时
full acceptance gate 能同时验收 DB size、market 覆盖率、raw archive 覆盖率
```

全量完成标准：

```text
本地 955 个 parquet 全部进入 inventory
所有 ready 小时均已物化
失败小时有明确 error reason
总覆盖报告生成
DEPTH / fill-first / price-only / unusable 分层完成
archive audit 的 ready_files = local_raw_files
archive audit 的 ready_raw_byte_pct = 100%
full acceptance gate 严格模式返回 0
raw 删除候选清单生成，但不自动删除
```
