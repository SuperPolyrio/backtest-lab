# OrderFilled-only 概率成交模型

## 边界

该模型的训练、校准、回测和策略输入只使用：

```text
trade_prints_one_sided
```

概率模型不会直接制造成交。订单最终仍必须满足：

```text
1. 到达时间之后存在真实同向 OrderFilled trade print。
2. 历史成交价格和价格 buffer 满足 limit。
3. 分配量不超过 trade participation capacity。
4. 同一 source trade 的容量不能被多个模拟订单重复使用。
5. 每笔 fill 保留 source_trade_id、tx_hash 和 source_log_indexes。
```

因此，概率的作用是：

```text
估计未来是否会出现真实可用成交，
而不是替代真实 OrderFilled 成交证据或重复压缩已经出现的成交。
```

## 特征

模型只读取订单到达前的数据：

```text
过去窗口同向/反向成交笔数
过去窗口同向/反向成交量
同向成交量占比
最近同向/反向成交距今时间
limit 相对最近成交价的距离
窗口价格变化
订单量与历史 tape 成交量的比例
execution horizon
BUY / SELL
价格距 0.5 的程度
```

到达后的数据不能进入概率特征，只能作为实际成交证据。

## 两阶段概率与容量

训练器使用 logistic regression 估计未来 horizon 内出现同向、limit-eligible OrderFilled 的概率，并使用 isotonic regression 校准概率。

第二阶段只在未来存在真实 source trade 的训练样本上学习：

```text
conditional_fill_fraction =
    min(1, future_eligible_volume * participation_rate / order_size)
```

运行时不再使用 `p_fill` 重复压缩成交量。最终成交容量为：

```text
min(
    order_remaining,
    historical_trade_size * participation_rate,
    conditional_capacity_order_cap,
    market_window_remaining_cap
)
```

低概率不会绕过真实成交证据。默认 profile 不做概率硬拒绝。

可选容量口径：

```text
probabilistic_conservative
    条件容量 25% 分位数，作为保守下界。

probabilistic_trade_tape
    条件容量均值，作为策略开发中心估计。

probabilistic_source_confirmed
    不额外压缩 order-level capacity，只受真实 source trade 的
    2.5% participation cap 和共享容量账本约束，作为可审计上界。
```

## 训练

```bash
conda run -n polyBots python scripts/calibrate_orderfilled_probability.py \
  --auto-markets 3 \
  --rows-per-market 8000 \
  --sample-stride 2
```

输出：

```text
config/execution/orderfilled_probability_profile.v1.json
runtime_outputs/fill_trade_validation/orderfilled_probability_calibration.json
```

数据按每个 market/asset 独立做时间切分：

```text
最早 60%：训练
中间 20%：isotonic calibration
最后 20%：时间外测试
```

## Forward validation

```bash
conda run -n polyBots python scripts/validate_orderfilled_probability.py \
  --rows-per-market 5000 \
  --orders-per-market 250
```

验证区间从各 market 的训练末尾之后开始。质量门包括：

```text
每笔 fill 必须有 source trade
source aggressor side 必须与订单方向一致
source block 必须不早于 arrival
BUY exec_price 不得高于 limit
SELL exec_price 不得低于 limit
三种两阶段容量口径和 legacy conservative 的覆盖率、成交量对比
```

## 主回测调用

```json
{
  "price_source": "orderfilled_block_close",
  "execution_price_mode": "ORDERFILLED_V2_TAPE",
  "execution_profile": "probabilistic_trade_tape"
}
```

前端可选择：

```text
OrderFilled conservative
OrderFilled expected
OrderFilled source-confirmed
```

## 当前真实数据结果

默认 profile 使用 23,982 个 OrderFilled-only 样本训练。

时间外概率指标：

```text
ROC AUC:           0.6922
Brier score:       0.1380
calibration error: 0.0436
candidate recall:  0.9193
```

独立 forward replay：

```text
orders:                              750
two-stage any-fill rate:             52.27%
legacy conservative any-fill rate:   50.93%
any-fill delta:                      +1.33 percentage points

conservative-quantile volume:        90.90
expected-capacity volume:            133.92
source-confirmed volume:             235.71
legacy conservative volume:          186.57

source-confirmed full fills:         334
source-confirmed partial fills:      58
source invariant errors:             0
```

条件容量时间外指标：

```text
expected fill-fraction MAE:          0.2369
25% quantile lower-bound coverage:   81.59%
```

三种口径共享相同的真实 source 门，因此 any-fill 数量一致；差异只体现在每笔订单能分配多少历史成交容量。策略研究应同时检查 conservative、expected 和 source-confirmed，不能只挑最赚钱的一档。它不表示模型能恢复当时完整盘口，也不支持 queue-accurate maker 回测。

## Fill-only V3 修订 Idea：从代理拟合到可持续校准

### 为什么旧验证不能直接证明真实成交概率

V3 运行时仍然只读取订单到达前的 `trade_prints_one_sided`，不会读取 LOB。
PML2 和 Nautilus 只允许在离线阶段提供执行代理标签或实现对照，不能被称为真实订单标签。

旧外部验证覆盖了 12,276 笔订单，但订单合同实际固定为：

```text
side = BUY
size = 10 shares
tif = FAK
label = PML2_IMMEDIATE_FAK_EXECUTABLE
```

现有 Paper-Live 配对样本则包含：

```text
BUY / SELL
QUOTE / SHARES
FAK / FOK
1-84.4 shares
```

两批数据不属于同一个订单总体。旧验证的总体 expected-order ratio 接近 1，也不能
替代分层校准；旧结果中深盘口明显低估、浅盘口明显高估，只是总体上相互抵消。

因此，旧 12,276 笔订单和现有 31 笔 Paper-Live 标签保留作历史诊断，不进入新版
V3 的最终验收。新版必须使用未参与模型选择的新日期、新 market 和新订单。

### V3 必须预测三个不同目标

禁止继续用一个概率同时表示 FAK 和 FOK：

```text
FAK_ANY_FILL
    P(订单到达后立即成交至少一部分)

FAK_CONDITIONAL_FRACTION
    E(成交比例 | FAK 已经成交)

FOK_FULL_FILL
    P(订单到达后立即完整成交)
```

对应输出：

```text
FAK expected_size
    = order_size
    * P(FAK_ANY_FILL)
    * E(FAK_CONDITIONAL_FRACTION)

FOK expected_size
    = order_size * P(FOK_FULL_FILL)
```

以下旧公式只能作为诊断字段，不能再作为 FOK 概率：

```text
P(any fill) * E(fill fraction | fill)
```

### 订单域合同

每个概率 artifact 必须声明并由运行时强制检查：

```text
supported_sides
supported_tifs
supported_amount_units
minimum_order_size
maximum_order_size
supported_order_to_tape_ratio
training_label
training_period
model_version
artifact_sha256
```

模型没有见过 SELL 时，SELL 必须返回 `MODEL_DOMAIN_ABSTAIN`，不能沿用 BUY 概率。
模型只用 FAK 标签训练时，FOK 也必须拒绝，不能把 FAK 概率改名为 full-fill proxy。

QUOTE BUY 需要先按真实订单合同换算成 shares，但必须保留原始 amount unit 作为特征
和审计字段。订单量不能固定为 10 shares，训练与验证都必须覆盖多个绝对数量和
`order_size / trailing_trade_volume` 区间。

### 防止反复调参的版本化验证协议

每个模型版本的数据用途互斥：

```text
development
    用历史 PML2/Nautilus 代理标签开发特征和模型结构。

calibration
    用较早的真实终态标签校准截距、斜率或概率曲线。

prospective acceptance
    模型发布后才产生的新订单，只用于验收该版本。

monitoring
    已验收版本上线后的漂移监控，只触发新候选版本，不改写旧结果。
```

一条订单不得同时参与模型选择和该版本的最终验收。每次订单提交前必须持久化：

```text
model_version
artifact_sha256
feature_contract
feature_snapshot_hash
predicted_probability
predicted_fraction
side / tif / amount_unit / normalized_size
selection_policy
submission_propensity
prediction_ts
```

真实终态随后追加，不能回写到下单前快照。

### 探测订单与选择偏差

只提交最容易成交的订单会造成几乎全是正样本，无法识别模型的排序能力。校准采样
必须在受控小额和风险上限内，覆盖：

```text
低 / 中 / 高预测概率
BUY / SELL
FAK / FOK
QUOTE / SHARES
小 / 中 / 大 order-to-tape ratio
不同 category、price bucket、TTE 和 activity regime
```

每笔订单记录被选中和提交的已知概率。只有在 logging policy 对目标 policy 有支持时，
才能使用 inverse propensity 或 doubly robust 估计；缺少支持的分层必须报告
`POPULATION_UNIDENTIFIABLE`，不能外推全市场准确率。

### 质量门

最低样本门仅用于防止明显不足，不代表模型已经准确：

```text
scored labels >= 200
positive labels >= 20
negative labels >= 20
independent markets >= 20
independent dates >= 10
```

概率质量门必须同时通过，而不是用 OR 放行：

```text
Brier score
Brier regret versus matching-contract base rate
calibration-in-the-large
calibration slope / log loss
FAK/FOK、BUY/SELL、size、price、category、activity 分层
置信区间和最小分层样本
abstention coverage
```

总体 expected-order ratio 只能作为容量一致性指标，不能替代概率校准。任一关键分层
出现数量级高估或低估时，即使总体比例接近 1，也不得晋升为中央模型。

### 质量门不使用跨样本固定 Brier 阈值

`Brier <= 0.20` 和 `0.85 <= E/O <= 1.15` 保留为 legacy 实验复现参数，
不再作为新模型的默认晋升规则。原因是 Brier 的绝对数值同时受事件
基准率、可分辨性和不确定性影响；不同 FAK/FOK、时间窗口和 market 组成不能
共用一个绝对分数。scikit-learn 的概率校准文档也明确提醒，Brier 同时混合
reliability、resolution 和 uncertainty，不能单独证明校准良好：
https://scikit-learn.org/stable/modules/calibration.html

新的默认规则是 reference-relative adaptive gate：

```text
1. 在每个 side x TIF 合同内计算基准率 climatology。
2. 计算 Brier skill = 1 - BS_model / BS_reference。
3. 按 market-day 聚类 bootstrap，保留同一 market 内订单的相关性。
4. 只有 Brier regret 的置信区间上界 <= 0，才证明优于合同基准。
5. 总体数量用 calibration-in-the-large 和 E/O 置信区间判断，
   不再只判断一个固定百分比区间。
6. 对每笔无条件成交比例计算 squared loss，并与同批样本的
   constant-fraction climatology 比较。总成交量对上了，但逐单数量分配错了，
   仍然不通过。
7. 同时输出 calibration slope 和带置信带的 reliability bins。
8. 对 category、price、activity、BUY/SELL、size 分层重复计算；
   样本不足时是 INSUFFICIENT_SAMPLE，不是 PASS。
9. 用 log loss 作为极端概率错误的防护门；如果其聚类置信区间
   证明显著劣于合同基准，即使 Brier 点估计好看也不通过。
```

自适应质量门使用四种状态：

```text
PASS
    Brier regret 置信区间上界 <= 0，且数量校准区间覆盖理想值。

INCONCLUSIVE
    没有检测到显著变差或数量偏差，但样本尚不足以证明优于基准。

FAIL
    置信区间证明概率误差显著更差，或 E/O / 数量偏差显著偏离 1。

INSUFFICIENT_SAMPLE
    正负标签或独立 market-day 不足，不做质量结论。
```

总体必须是 `PASS`。关键分层允许显式的 `INCONCLUSIVE`，但不允许
`FAIL`；对一个分层的 `INCONCLUSIVE` 不得宣称该分层已单独校准通过。
`INSUFFICIENT_SAMPLE` 不进入分层通过率的分子或分母，也不能写成
“没有失败所以通过”。
自适应模式不再使用“80% 分层通过率”这类人为百分比；它要求每个
可评分维度至少有一个有效分层，且任何样本充足的分层都不得出现
统计显著的 `FAIL`。旧的 80% 规则只用于复现 legacy 报告。
时间分层与运行时模型保持一致，使用 4 小时 UTC session，而不是把每个
单独小时拆成只有十几个 market 的伪精细分层。

Brier 是 proper scoring rule，模型选择必须基于概率分数本身，而不是先把概率切成
0/1 再算准确率。Gneiting 与 Raftery 对 proper scoring rules 的定义和性质给出了
这一做法的统计基础：
https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf

Brier skill 使用参考预测的做法来自成熟的概率预测验证；ECMWF 的概率预报指南
同样以 climatology 作为 Brier skill 参考，而不是规定一个跨数据集通用的绝对
Brier 上限：
https://confluence.ecmwf.int/spaces/FUG/pages/673551875/Section%2B12.B%2BStatistical%2BConcepts%2B-%2BProbabilistic%2BData

Brier 本身也有抽样不确定性，因此新门禁保存聚类 bootstrap 区间，而不是把点估计
的小数点后三位当成确定结论：
https://journals.ametsoc.org/doi/10.1175/2007WAF2007049.1

校准不能只看 E/O。成熟的外部验证同时报告 calibration-in-the-large、
calibration slope、E/O 及置信区间，并用平滑校准曲线诊断局部偏差：
https://www.bmj.com/content/380/bmj-2022-071058

总体校准还不足以保证每个市场子群都可靠；多校准研究同样要求检查多个可识别
subpopulation，而不是允许不同分层的正负误差互相抵消：
https://proceedings.mlr.press/v80/hebert-johnson18a.html

因此，`101.6%` 的订单量比只说明总体偏差小；它不能抵消逐单 Brier、
校准斜率或分层失败。新实现必须将点估计、基准分数、置信区间和样本支持数
一起保存，不允许只保存一个 PASS/FAIL。

### 运行结果分账

V3 必须继续区分：

```text
SOURCE_CONFIRMED
    有真实未来 OrderFilled 证据的审计下界。

MODELED_EXPECTATION
    到达时概率模型的期望成交，只进入期望 PnL。

MODELED_MONTE_CARLO
    固定随机种子的情景成交，只进入分布分析。

MODEL_DOMAIN_ABSTAIN
    订单合同或特征落在训练支持域之外。
```

模型校准不能把 `MODELED_*` 改写成 observed fill，也不能使用订单自己的未来
`OrderFilled` 事件提高到达时预测。

### 新版开发与验收顺序

```text
1. 修复 FAK/FOK 目标合同和运行时域检查。
2. 为 BUY/SELL、FAK/FOK、不同数量生成匹配合同的 PML2 代理训练集。
3. 分别训练 any-fill、conditional-size 和 full-fill 模型。
4. 使用过去真实终态只做校准，不回收为同版本验收数据。
5. 从全新日期和 market 构造至少 10,000 笔离线订单做广度验证。
6. 每个关键分层单独过门，不接受总体误差抵消。
7. 发布版本后收集新的前瞻 Paper-Live 标签。
8. 只有未来标签通过质量门，才允许声明真实订单概率可迁移。
```

最终目标不是让一个固定参数永远准确，而是让每个模型版本具有清楚的训练范围、
前瞻证据和失效条件。新数据可以触发下一版本，但不能反过来修改旧版本已经报告的
概率和回测结果。

### 2026-09-03 实现与独立离线验收

代码默认已经切换到 adaptive gate。固定的 `Brier <= 0.20`、
`0.85 <= E/O <= 1.15` 仍可通过 `quality_gate_mode=fixed` 复现旧报告，但不再是
校准器、单 artifact 验证器或万人级交叉验证器的默认规则。

实现入口：

```text
quant/backtest/probability_quality.py
scripts/validate_fill_only_v3_probability_artifact.py
scripts/validate_fill_only_v3_cross_window_stability.py
scripts/run_fill_only_v3_large_cross_validation.py
scripts/calibrate_fill_only_v3_hierarchical_overlay.py
scripts/calibrate_fill_only_v3_l2_reference.py
```

FAK 与 FOK 已按不同订单合同分别训练和检查。两批验证都覆盖 BUY/SELL、
`1/5/10/25/100` shares、多类别和多个 market-day；运行时仍然只使用到达前的
OrderFilled 特征。

```text
FAK_ANY_FILL:
    orders                         17,100
    windows                        12
    independent market-days        180
    model Brier                 0.205643
    climatology Brier           0.225088
    Brier skill                  8.6387%
    Brier skill 95% CI       [4.5962%, 12.1515%]
    expected-order ratio         0.961901
    order-ratio 95% CI       [0.906795, 1.021194]
    expected-quantity ratio      0.998623
    quantity-ratio 95% CI    [0.930947, 1.072747]
    adaptive status                  PASS

FOK_FULL_FILL:
    orders                         13,800
    windows                        10
    independent market-days        150
    model Brier                 0.201224
    climatology Brier           0.231421
    Brier skill                 13.0488%
    Brier skill 95% CI       [8.2268%, 17.7989%]
    expected-order ratio         0.979730
    order-ratio 95% CI       [0.920242, 1.056392]
    expected-quantity ratio      1.013387
    quantity-ratio 95% CI    [0.934130, 1.115884]
    adaptive status                  PASS
```

这也给出了固定 `0.20` 阈值的反例：FOK 的 Brier 为 `0.201224`，按旧规则会失败；
但它相对同合同无信息基准显著改善，且订单量、成交量和总体校准的 95% 区间都覆盖
理想值。相反，一个绝对 Brier 小于 `0.20` 的模型若显著差于其低基准率合同的
climatology，仍会失败。

验收产物：

```text
backtest_framework/nautilus_trader_comparison/
    fill_only_v3_contract_fak_adaptive_v22_external_aug30_sep1_combined/
        model_artifact.json
        adaptive_validation.json
    fill_only_v3_contract_fok_adaptive_v14_external_market_group_combined/
        model_artifact.json
        adaptive_validation.json
```

对应模型 artifact SHA-256：

```text
FAK v22  2d4c6b3881f02351da06dec0cb046112726cbb4efbe8435927f77c84956e3b2c
FOK v14  0c158c60d91c995972f71a8574c517ce725fdd438f7591fcefb4f4dc9758a085
```

运行时不会默认改写旧回测口径。新模型通过独立 profile 显式选择：

```text
central_trade_only_contract_aware_adaptive
    FAK -> adaptive_v22 / FAK_ANY_FILL
    FOK -> adaptive_v14 / FOK_FULL_FILL

taker_arrival_contract_probability_only_adaptive
    只输出到达时概率，不允许未来 source trade 提升结果，用于后续 Paper/Live 标签校准。
```

两个 artifact 在 profile 中固定 SHA-256；加载时哈希不一致将直接
fail closed，避免模型文件变化后仍沿用旧 profile 名称。旧
`central_trade_only_contract_aware` 保留不变，只用于复现旧结果。

PML2 与 Nautilus 在这两批即时 FAK/FOK 标签上一致，所以它们是两个实现对照，
不是两份独立真实下单证据。以上 `PASS` 只允许把模型用于本地 OrderFilled-only
期望成交研究；不能据此宣称真实订单成交概率已经完成实盘校准。
