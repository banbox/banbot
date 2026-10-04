# 因子与截面 API


本页介绍 `factor`、`factor/expr`、`factor/research` 和 `factor/runner`。配置和可运行流程见 [多因子与截面策略](../guide/factor.md)。

| 包 | 主要组件 | 边界 |
| --- | --- | --- |
| factor | Plan、Session、Batch、VersionStore、Snapshot、Universe、RoundBarrier、TargetPortfolio | 冻结可见数据与增量/批量计算，不管理交易所 |
| factor/expr | Spec、Compile、CloneSpec | 启动编译声明；完整校验包括未使用声明 |
| factor/research | ComboSpec、ICHistory、LabelQueue、ManifestSpec | 成熟标签/历史统计独立于推理 |
| factor/backtest | Book | weights 近似数量账本，不替代 execution 账户 |
| factor/runner | Config、Run、NewLive、ComputationGroup、AccountSink | replay/live 驱动和账户目标投递 |

## 注册与编译

`runner.RegisterDefinition(name string, builder runner.DefinitionBuilder) error` 注册 Go 图；`DefinitionBuilder` 是 `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`。同名重复注册失败。内置定义为 momentum-vol。

`runner.RegisterPortfolioBuilder(name string, builder runner.PortfolioBuilder) error` 注册自定义目标生成器；builder 接受 Frame、Universe、PortfolioSpec、PortfolioDefinition，返回目标、诊断和 error。唯一版本名不能使用保留的 top-bottom-k-v1。

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

Plan、ComputationGroup、PortfolioBuilder、HistoricalInput、ObserveBatch 和内部 timeline 保持借用身份，不创建账户或读输入。复制不代替 ValidateReplayConfig/ValidateLiveConfig；借用 owner 应在所有 driver Join 后释放资源。expr.CloneSpec、research.CloneComboSpec、research.CloneManifestSpec 提供对应配置容器复制。

## replay 与 live

Run 是历史驱动；NewLive 创建实时时钟/已对账 sink 驱动，Observe 接收当前数据，Flush 排空决策 barrier。Stop 禁止新输入，Join 等待已接纳回调和计算后释放共享 Session。共享计算不会共享目标、预算或研究状态。

嵌入式实时装配使用 runtime.CompileFactorsLivePlan、SubscribeFactorsLive、InstallFactorsLive、BindFactorsLive；入口以 entry.RegisterFactorLiveBinding 注册已验证会话证据。它不是单纯的 YAML 开关；缺 transport、metadata、revision/funding 或账户证据会失败。当前无完整真实 venue 验收。

## 数据与输出

VersionRecord 保存事件、可见性/接收时间、修订和有类型 Values。Snapshot getter 的复制是所有权边界，禁止以性能优化移除任意字段和 NULL。Session 与 Batch 的递归历史语义不同，不互相替代。

Manifest 记录代码、图、Universe、可见性、输入引用、标签和执行假设；它不同于原始记录。Result.Unresolved 保留未成熟标签。事件/posting Gob 是账户审计输出，时序 orders.gob 是兼容报告，两者不互换。
