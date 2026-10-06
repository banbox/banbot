# 因子与截面 API

CLI 启动已统一：根命令 `backtest` / `trade` 从同一 YAML `RunSpec` 调度因子、时序或混合策略；根命令 `research`、`validate --spec`、`explain --spec` 与 `data archive` 分别提供研究、独立表达式编译和归档转换。下述嵌入 API 契约保持原用途。


本页介绍 `factor`、`factor/expr`、`factor/research` 和 `factor/runner`。配置和可运行流程见 [多因子与截面策略](../guide/factor.md)。

| 包 | 主要组件 | 边界 |
| --- | --- | --- |
| factor | Plan、Session、Batch、VersionStore、Snapshot、Universe、RoundBarrier、TargetPortfolio、PortfolioTarget、PortfolioPolicy | 冻结可见数据与计算、持仓生命周期提案，不管理交易所 |
| factor/expr | Spec、Compile、CloneSpec | 启动编译声明；完整校验包括未使用声明 |
| factor/research | ComboSpec、ICHistory、LabelQueue、ManifestSpec、模型/风险/实验接口 | 成熟标签、历史统计和研究产物与当期推理分离 |
| factor/backtest | Book | weights 近似数量账本，支持 allocation，不替代 execution 账户 |
| factor/runner | Config、Run、NewLive、ComputationGroup、AccountSink | replay/live 驱动和账户目标投递 |

## 注册与编译

`runner.RegisterDefinition(name string, builder runner.DefinitionBuilder) error` 注册 Go 图；`DefinitionBuilder` 是 `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`。同名重复注册失败。内置定义为 momentum-vol。

`runner.RegisterPortfolioBuilder(name string, builder runner.PortfolioBuilder) error` 注册自定义目标生成器；builder 接受 Frame、Universe、PortfolioSpec、PortfolioDefinition，返回目标、诊断和 error。唯一版本名不能使用保留的 top-bottom-k-v1。

`runner.RegisterPortfolioPolicy(name string, factory runner.PortfolioPolicyFactory) error` 注册完整有状态策略，名称须带版本（例如 `my-policy-v1`），同名注册失败。每个 run/策略调用工厂获得独立实例；内置名称为 `lifecycle-v1`。builder 生成理想组合，policy 根据真实持仓与在途证据形成可接纳目标。显式 builder 未配置 allocation 时保留其理想权重。

`runner.CompileDefinition(c runner.Config)` 与 replay/live 使用相同定义解析。表达式 Config.Expressions 与 Plan/Definition 互斥。节点的 schema、缺失策略和版本构成可复现图身份。

## 可编译的 Go definition

把以下代码放入自己的策略项目入口，编译后才能使用 CodeMomentumV1。它包含两列原生因子输出并明确处理注册错误：

```go
package main

import (
    "fmt"
    "github.com/banbox/banbot/entry"
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/research"
    "github.com/banbox/banbot/factor/runner"
)

func buildMomentum(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
    if c.Factor.Source == "" || c.Factor.Field == "" || c.Factor.TimeFrame == "" || c.Factor.Window < 2 {
        return nil, research.ComboSpec{}, fmt.Errorf("source, field, timeframe and window >= 2 are required")
    }
    price := factor.Positive(factor.Field(c.Factor.Source, c.Factor.Field, c.Factor.TimeFrame))
    returns := factor.Return(price, c.Factor.Window)
    plan, err := factor.New().
        Add("momentum", factor.ZScore(returns)).
        Add("momentum_rank", factor.Rank(returns)).
        Compile()
    return plan, research.ComboSpec{
        Method: research.Equal,
        Columns: []string{"momentum", "momentum_rank"},
    }, err
}

func main() {
    if err := runner.RegisterDefinition("CodeMomentumV1", buildMomentum); err != nil {
        panic(err)
    }
    entry.RunCmd()
}
```

YAML 用下段替换内置策略的 policy，其他市场、历史数据和执行设置仍需提供。definition 与 expressions 互斥：

```yaml
run_policy:
  - name: custom_momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 1}
    definition: CodeMomentumV1
```

## 配置所有权

`runner.CloneConfig(c Config) (Config, error)` 复制 chunks、snapshot 的 Universe 列表和 maps、expressions、combo、manifest 和 execution instruments。顺序、重复项、nil/空容器区别保留；一次 JSON 序列化校验保留 NaN/Inf 拒绝。

Plan、ComputationGroup、PortfolioBuilder、PolicyContext、HistoricalInput、ObserveBatch 和内部 timeline 保持借用身份，不创建账户或读输入。复制不代替 ValidateReplayConfig/ValidateLiveConfig；借用 owner 应在所有 driver Join 后释放资源。expr.CloneSpec、research.CloneComboSpec、research.CloneManifestSpec 提供对应配置容器复制，policy 配置中的 maps、覆盖指针和 policy_params 也拥有副本。

## 生命周期提案与目标

```go
type PortfolioPolicy interface {
    Propose(factor.PortfolioContext, json.RawMessage) (factor.PortfolioProposal, error)
}
```

这是 `factor.PortfolioPolicy` 的接口签名。`PortfolioContext` 提供冻结 Frame/Universe、理想组合、PortfolioSpec、策略 NAV、只读 Positions、Marks、StateVersion、LedgerCursor 和 Previous。`Config.PolicyContext` 可补充当时可见的 Groups、Volatility、Beta、ForceExit、RebalanceDue、HoldingRules 和 TransitionRules。资产显式 `by_asset` 覆盖优先于 resolver 规则，再回退默认规则。参数产物先通过 `research.ResolveParameters` 验证 hash、manifest、训练截止和可用时间。

`PositionEvidence.FirstFillTime` 与 `FillEvents` 来自真实成交；目标生成、接纳、订单提交不增加持仓年龄。`PortfolioProposal` 返回 Target、NextState、Reasons、AcceptanceID 和需取消增仓的 ReconcileSIDs。NextState 必须是合法 JSON，大小受 `factor.MaxPortfolioStateBytes` 限制；自定义 opaque JSON 不套用内置 LifecycleState schema。没有目标但改变 checkpoint 的提案也必须有 AcceptanceID。

`factor.NewPortfolioTarget(spec, allocations)` 创建版本 1、内容寻址的不可变目标；getter 返回拥有副本。每个 SID 的 Allocation 使用有限普通十进制字符串 Value，Basis 为：

| Basis | 含义 |
| --- | --- |
| `nav-fraction` | 有符号策略 NAV 权重 |
| `absolute-quantity` | 有符号标准资产数量，不是合约张数 |

Full 将遗漏的旧资产目标归零；Patch 保留遗漏目标。身份包含策略、账户、预算、计划序号、冻结快照和有效窗口，policy 不能修改已冻结身份。`PortfolioTargetFromWeights` 可适配旧 TargetPortfolio；`AsWeightPortfolio` 仅接受纯权重目标，绝对数量不能隐式降级。消费数量目标的输出须实现 `runner.AllocationOutput`；JSONL 使用 `allocation-decision` / `allocation-accepted`。

`runner.PolicySink` 提供 `PolicyEvidence` 和 `AcceptProposal`。weights 的 Book 自带证据与接纳；events/live 由账户 owner 提供。纯 research 没有默认持仓账本，显式开启 policy 时嵌入调用者必须提供 allocation-capable PolicySink；普通 CLI `research` 不能仅靠 YAML 获得此能力。

接纳在 owner 内原子提交计划、目标和 checkpoint，并校验 StateVersion/LedgerCursor；证据变化则使用同一冻结 Frame 重新提案。无交易状态更新也走事务。重复接纳校验内容，冲突拒绝；恢复读取持久序号、checkpoint、前目标及成交来源。接纳后发送失败仍保留已接纳 receipt，不能回滚状态或伪造成交。

实盘执行范围保留实际持仓、在途订单和活跃 cohort 的执行价格/funding 订阅。退出选股池不会丢弃尾仓；归零且结算后才释放。cohort 记录聚合执行贡献并支持守恒的内部净额转移；独立逐批 execution lot、严格逐批成交期限和逐批 PnL 尚未提供。

内置配置默认值、模式及示例见[生命周期指南](../guide/factor.md#独立调仓与持仓生命周期)。未显式启用 policy 时，旧 builder、纯权重合同和配置身份继续保留。

## 多期限与历史合成

`Manifest.Labels` 可声明多个 executable-return horizon，各自捕获执行价、成熟并报告未完成标签，共享冻结 Frame、有界队列。`ComboSpec.Label` 指定历史权重使用的标签；多标签省略时选择最短 horizon，同期限按名称排序，选择纳入 manifest；单标签省略保留原身份。

除 equal/fixed/history-ic 外，支持 history-rank-ic、history-icir、history-rank-icir 和 history-ewma。MinSamples 是历史截面数，MinPairs 是每截面有效资产对数；MinConfidence 是未经重叠调整的均值/标准误门槛。Decay 为 EWMA alpha，零值取 0.2；Direction 默认 signed，可选 positive；Fallback 默认 equal，可选 fixed/error。入库与选权重均检查决策时间、成熟和可见性。live 尚无实时成熟历史 provider，拒绝全部 history 方法。

`ColumnMetrics.RankAutocorrelation` 比较相邻参考截面，`Decay()` 提供分期限 IC；Accumulator 独立维护 column/label 顺序，允许较旧长周期样本晚于较新短周期样本成熟。重叠收益显著性使用 `research.HACMean(values, lag)`，lag 需按实际采样/持有周期选择。

## 原生 Go 研究扩展

| API | 用途与边界 |
| --- | --- |
| `factor.RobustZScore` / `MADWinsorize` / `MultiResidual` / `WeightedResidual` | Reference 池的稳健变换、多暴露 OLS/WLS；对应表达式见[指南](../guide/factor.md#稳健表达式与研究扩展) |
| `runner.ScanPortfolioTrials` | 有上限、独立配置/状态的组合扫描，不能把全样本最优带入过去 |
| `research.SelectParameters` / `ResolveParameters` | episode 独立样本、组/全局收缩和 PIT 参数产物；候选参数纳入内容 hash |
| `research.RegisterModel` / `FitModel` / `RestoreModel` | 每次 fit 创建 trainer，Predictor 只接特征；内置含截距 ridge/OLS，ridge 不惩罚截距 |
| `VisibleTrainingRows` / `RollingWindows` | 决策时特征可见、标签成熟、purge/embargo 与有界滚动窗口 |
| `PublishModel` / `LoadModel` | 临时文件、Sync/Rename 发布，恢复验证内容 hash 与 PIT；不是自动线上训练服务 |
| `runner.RegisterModelPortfolioBuilder` | 注册已发布模型选股 builder，配置 hash 纳入 StrategyHash；无可见模型时跳过新目标 |
| `research.EstimateCovariance` / `OptimizePortfolio` | 对角/收缩协方差、PSD 检查与有界投影优化；分别报告可行性、收敛和 violations |
| `runner.RegisterRiskPortfolioBuilder` | 注册风险分配 builder，冻结配置及 hash；预拟合矩阵须提供 SIDs/TrainingEnd/AvailableAt |
| `SummarizeLifecycles` / `SummarizeCapital` / `EstimateCapacityCost` / `Attribute` | 真实持仓/退出延迟、资本、成本与 benchmark/阶段归因；调用者提供可核验事件 |
| `FactorRegistry` / `OpenTrialLedger` / `CompareTrials` / `RandomBaselineScores` | 版本化元信息、JSONL trial、分离训练/样本外指标及可重复随机基线 |
| `NewResearchCache` | 字节预算 LRU，读写复制；identity 含 manifest/算法/schema/universe/revision/窗口 |

模型 builder 产生预测理想组合；lifecycle 的 retain/dropout 默认仍比较基础 score，使用预测排名须通过 PolicyContext 提供对应分数。风险 builder 无历史状态，换手限制由 policy 或 OptimizePortfolio.Previous 承担；不可行结果不能直接作为目标，也不承诺全局最优。第三方 ML/求解器需要注册扩展，没有新增默认依赖。

报告不从目标接纳时间推断成交时长；聚合 lot 无法证明逐批归因时不能伪造 BatchID。普通 CLI 不自动采集完整逐批成交报告。trial 损坏/内容冲突会拒绝恢复且保留文件，多进程写入需外层 owner 串行管理；CompareTrials 不按样本外表现排序做模型选择。

实现与更完整 Go 用法见仓库代码路径 `doc/factor_opt_implementation.md`、`doc/factor_research_extensions.md`。真实成本、容量与模型有效性须独立样本外验证。

## replay 与 live

Run 是历史驱动；NewLive 创建实时时钟/已对账 sink 驱动，Observe 接收当前数据，Flush 排空决策 barrier。Stop 禁止新输入，Join 等待已接纳回调和计算后释放共享 Session。共享计算不会共享目标、预算或研究状态。

嵌入式实时装配使用 runtime.CompileFactorsLivePlan、SubscribeFactorsLive、InstallFactorsLive、BindFactorsLive；入口以 entry.RegisterFactorLiveBinding 注册已验证会话证据。它不是单纯的 YAML 开关；缺 transport、metadata、revision/funding 或账户证据会失败。当前无完整真实 venue 验收。

## 数据与输出

VersionRecord 保存事件、可见性/接收时间、修订和有类型 Values。Snapshot getter 的复制是所有权边界，禁止以性能优化移除任意字段和 NULL。Session 与 Batch 的递归历史语义不同，不互相替代。

Manifest 记录代码、图、Universe、可见性、输入引用、标签和执行假设；它不同于原始记录。Result.Unresolved 保留未成熟标签。事件/posting Gob 是账户审计输出，时序 orders.gob 是兼容报告，两者不互换。
