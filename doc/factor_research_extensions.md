# 因子研究扩展与原生 Go 接口

本页说明 `factor_opt.md` 的 P1/P2 研究接口。普通配置继续使用现有 `engine: factor`、表达式和组合配置；模型、风险优化和试验接口作为 Go 扩展提供，不需要新增 ML/求解器依赖。所有价格、暴露和分组字段继续从 `DataSeries.Values` 读取。

## 稳健变换和中性化

表达式新增：

```text
cs.mad_winsorize(kline.close, 3)
cs.robust_zscore(kline.close)
group.ols(factor.signal, style.size, style.beta)
group.wls(factor.signal, style.weight, style.size, style.beta)
group.demean(factor.signal, "style", "industry")
group.zscore(factor.signal, "style", "industry")
```

`style` 必须在 bindings 中声明；较慢暴露源使用已有 asof binding，并提供 `max_age_ms`。行业支持原始字符串/其他可序列化字段及 NULL。统计拟合只使用冻结的 Reference 池，然后应用到活跃资产；Evaluation 池不参与拟合。

MAD 使用参考中位数和 `1.4826 × median(abs(x-median))`。MAD 为零时 robust zscore 返回零，MAD winsorize 返回中位数；它们不把 NULL 变成数值。Go 对应 `factor.RobustZScore`、`MADWinsorize`、`MultiResidual`、`WeightedResidual`。多暴露 OLS/WLS 含截距，WLS 拟合权重必须为正。求解使用两次重正交 QR，按声明顺序丢弃线性相关暴露，样本不足返回 Warmup。

## 成熟历史合成和信号衰减

组合新增 `history-rank-ic`、`history-icir`、`history-rank-icir`、`history-ewma`。`ComboSpec.Label` 选择驱动中的成熟标签；单标签省略时保留原身份，多标签省略时选最短 horizon，同期限按名称排序，并将选择写入 manifest。`MinSamples` 是截面样本数，`MinPairs` 是每个截面有效资产对数。`MinConfidence` 是均值/标准误阈值，`Decay` 是 EWMA alpha，零值使用 0.2；`Direction` 为 signed（默认）或 positive；`Fallback` 为 equal（默认）、fixed 或 error。

这些模式继续受原有 live 禁止使用历史 IC 的规则约束。所有样本在入库和选权重时检查成熟、可见与原始决策时间，不通过未来标签筛选当期证券池。置信度是未经重叠调整的门槛，不能据此宣称独立显著性。重叠收益研究可使用 `HACMean(values, lag)`，lag 由实际持有/采样周期决定。

`ColumnMetrics.RankAutocorrelation` 比较上次与本次冻结参考值，`Decay()` 提供分期限 IC；`RankAutocorrelation` 也可单独调用。Accumulator 各 column/label 维护独立顺序，允许短期限的较新决策先于长期限的较旧决策成熟，同期限重复报告仍拒绝。

## 执行生命周期、成本、归因

`SummarizeLifecycles([]TradeLifecycle)` 消费真实入场/退出请求/退出成交事件，按资产、分组和批次给出持仓时长、实际退出延迟、费用、冲击、funding、成本后收益和累积 PnL 回撤。输入收益和费用必须来自同一冻结 NAV 口径；funding 可为负数（收入）。它不把目标接纳时间伪装成成交年龄。默认聚合 lot 无法提供真实逐批成交归因时，调用者不应伪造 BatchID 事实。

这些报告是原生 Go API，事件由调用者从可核验账本提供；普通 CLI 不会自动采集完整逐批成交归因报告。

`EstimateCapacityCost` 使用报价币计价成交额与 ADV，输出手续费、滑点、平方根冲击、资金费率和最大参与率可行性。Funding 用带符号的持有名义额计算。成本是预估；执行器仍负责真实订单与费用。

`Attribute` 提供 benchmark 超额和 selection/transition/sizing/执行成本的加法分解，保留未解释 residual。style/group 是独立归因视图，不能与上述阶段再次求和以免重复计数。

## 持仓参数研究产物

`SelectParameters(observations, spec)` 接受训练窗口内成本后周期实验观测。它按非重叠 episode 计独立样本，对稀疏资产先使用组/全局建议，达到最低独立样本后按 prior sample 数向上级均值收缩。不同候选按成本后均值减 `ConfidencePenalty × HAC标准误` 比较；候选按名称稳定排序，避免随机 map 顺序影响选择。

产物包含 ManifestID、算法版本、训练起止、样本截至、可用时间、扫描次数及 scope。可通过 `ParameterSelectionSpec.Candidates` 声明候选名称到数值参数的映射，例如 `{"16h":{"min_bars":16,"exit_steps":8}}`，可见候选参数拥有副本并纳入产物 hash。`ResolveParameters` 验证 content hash，只加载推理时已发布且训练结束的匹配数据 manifest 版本。选择窗口不包含测试窗口；测试结果应单独写 trial 的 `OutOfSample`。观测的持仓规则模拟与账本事件采集由既有 runner/policy 或用户扩展完成，本接口不复制一套撮合引擎。

## fit/predict、滚动和模型恢复

原生扩展契约为 `RegisterModel(versionedName, ModelFactory)`、`Trainer.Fit/Restore`、`Predictor.Predict/Snapshot`。工厂每次 fit 创建独立 trainer；推理接口只接特征，不能接未来标签。

```go
artifact, predictor, err := research.FitModel(
    "linear-ridge-v1", manifestID, []string{"momentum", "volatility"},
    json.RawMessage(`{"Ridge":0.01}`), samples, window, publishedAt,
)
if err != nil { return err }
prediction, err := predictor.Predict([]float64{momentum, volatility})
path, err := research.PublishModel(modelDirectory, artifact)
_, restored, err := research.LoadModel(path, inferenceAsOf)
```

内置基线是含截距 ridge/OLS；ridge 不惩罚截距。`VisibleTrainingRows` 检查特征在原始决策时已发布、标签成熟及 fit 截止可见，purge 会排除触及测试区间的标签，embargo 在测试之前保留隔离区。`RollingWindows` 构建有界滚动训练/测试窗口；调用者按窗口 FitModel，按模型可见时间切换推理。

模型产物保存特征顺序、训练窗口、样本截至、样本数、版本、参数、payload 和内容 hash。发布使用临时文件、Sync、Rename，重复发布验证已有内容；恢复校验 hash/PIT。没有自动线上训练服务、数据库模型中心或第三方 ML 后端，自定义模型可实现同一原生接口。

运行引擎也可通过 `runner.RegisterModelPortfolioBuilder(name, ModelBuilderConfig{ManifestID: trainingManifestID, Artifacts: artifacts})` 注册模型选股 builder，并在 `portfolio.builder` 选择注册名字。每轮从已发布模型中选最新版本，用冻结 Frame 中与产物同顺序的特征 Predict，再复用原 selector。未来版本不参与预测，缺少当期可见模型则跳过新目标。模型 builder 的配置 hash 纳入 StrategyHash。它产生预测的理想目标；lifecycle 的 retain/dropout 默认仍比较基础组合 score，如需使用预测排名，应同时用 `Config.PolicyContext` 显式提供对应分数。

## 风险模型与优化 builder

`EstimateCovariance(timeRows, shrinkage, diagonal)` 估计同资产顺序的样本协方差，支持对角模型与向对角矩阵收缩。`OptimizePortfolio` 是有界 projected-gradient 基线，检查 PSD，支持 gross、单资产、分组、net、beta、同周期波动率目标和目标单边换手。`LimitNet/LimitBeta/LimitTurnover` 区分零上限与未启用；long-only 显式配置。结果包含 Converged、Status、Violations、现金和剩余约束信息。不能把 infeasible 结果当普通目标，也不会静默放松换手。

原生运行 builder 可用：

```go
err := runner.RegisterRiskPortfolioBuilder("my-diagonal-risk-v1", runner.RiskBuilderConfig{
    ScoreColumn: "score", VolatilityColumn: "volatility",
    RiskAversion: 2, Gross: 1, MaxWeight: 0.1, LongOnly: true,
})
```

然后在 `portfolio.builder` 选择该名字。预拟合协方差还需声明稳定 SIDs、TrainingEnd、AvailableAt；未来风险矩阵拒绝使用。注册配置会深复制，其规范化 hash 纳入 StrategyHash，并在目标诊断记录。这个 builder 无历史状态，换手约束由有状态 policy 或直接 OptimizePortfolio 的 Previous 输入承担。基线不承诺全局最优；是否收敛与可行性分别报告。

## 元信息、试验目录和缓存

`FactorRegistry.Register/Snapshot` 保存版本化因子名称、方向、说明、数据源和 tags，返回拥有副本。`OpenTrialLedger(path, maxTrials)`/`Append` 提供带上限、内容寻址、幂等追加的 JSONL 试验目录。每条 trial 包含 manifest、算法版本、参数、seed、baseline 标识、扫描次数、训练/测试窗口和分离的训练/样本外指标。torn record 或冲突拒绝恢复并保留原文件；不会悄悄丢弃证据。一个 ledger 句柄支持并发调用；多进程写同一文件需由外层 owner 串行管理。

`CompareTrials` 稳定展示训练与样本外指标，不按测试表现排序做模型选择。`RandomBaselineScores(frozenSIDs, seed)` 提供只依赖冻结成员和 seed 的可重复随机排名；用户将这些分数送入相同 selector，trial 记录 seed 和 baseline 名称。每轮重新随机时，seed 日程应显式记录。

`NewResearchCache(maximumBytes)` 是按字节预算的 LRU，读写均复制 bytes。CacheIdentity 必须包含 manifest、算法、schema、universe、源 revision 与窗口，任一变更产生不同 key。预算包含 key 和 value 长度，不承诺覆盖 Go map/list 本身的常数开销；不替代 Session/Batch，也不保留无限面板。

## 验证

定向测试覆盖稳健数值、共线暴露、组表达式、成熟历史权重、未来标签扰动、模型 hash/PIT/恢复、硬约束不可行、capacity/funding、生命周期、trial 幂等/损坏恢复、参数收缩与 LRU 失效。运行：

```sh
go test ./factor ./factor/expr ./factor/research ./factor/runner
go vet ./factor/...
```

真实数据容量、模型有效性和成本参数需要独立样本外研究；本实现的确定性单元测试不会给出“最佳持仓周期”或性能胜于外部框架的结论。
