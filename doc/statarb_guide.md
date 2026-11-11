# 统计套利研究与运行指南

当前实现提供统计研究工具、可复现因子运行和账户执行基础设施。使用的基本单位是
“可见数据 → 冻结训练产物 → 当期信号 → 目标组合 → 成交与账户证据”。研究函数
本身不会自动创建交易策略；策略需要显式说明训练区间、信号方向、风险预算和退出规则。

> **最短入口**：配对统计套利使用 `runner.RegisterPairs`（滚动回归 + z-score 滞回 + β 对冲，一次注册、YAML 只写 `name`），截面因子使用 `runner.RegisterFactor`。详见[因子指南](../bandoc/zh-CN/guide/factor.md#用-go-写策略三种入口)。下方数值工具用于需要自定义训练、检验与组合的研究。

## 从哪里开始

| 需求 | 使用入口 | 支持范围 |
| --- | --- | --- |
| 检验配对、过滤价差、研究标签或模型 | [statistics 主题索引](../factor/statistics/README.md) | 原生 Go 数值与研究 API |
| 训练、参数选择、发布和恢复产物 | [研究扩展指南](factor_research_extensions.md) | 成熟标签、训练/测试隔离、内容 hash 和发布时间校验 |
| 从 YAML 做 research / weights / events | [runner 指南](../factor/runner/README.md)、[完整配置](../bandoc/zh-CN/guide/factor.md) | 已注册 Go definition、表达式与 portfolio builder |
| 接入有状态策略 | [definition.go](../factor/runner/definition.go) | 每次运行独立 factory；与成交提交同步的状态使用 PortfolioPolicy |
| 期权估值与模拟 | [options.go](../factor/statistics/options.go)、[配置说明](../factor/runner/README.md#yaml-clock-settlement-and-paper-fill-choices) | 估值 API 与现金结算欧式期权 paper Events 是不同能力 |
| 跨场所机会与模拟账本 | [机会计算](../factor/statistics/chan_crossvenue.go)、[CrossVenuePaper](../execution/crossvenue.go) | 独立 Go paper 实验，预先分配各场所现金/库存；无跨场所原子实盘保证 |
| 实盘账户接入 | [live 适配器](../entry/factor_banexg_live.go)、[runner 指南](../factor/runner/README.md) | 需要真实传输、元数据、账户快照、数据版本与资金费证据 |

## 最短 Go 研究链

下面程序只在训练样本上拟合对冲系数和尺度，然后对一个新观察计算标准化价差。
它使用合成数据演示调用组织方式；ADF 统计量不直接解释为通过协整检验，也不生成订单。
真实输入应由调用方从当时可见的 `DataSeries.Values` 读取，保留其他字段的类型和 NULL。

```go
package main

import (
    "fmt"
    "math"

    "github.com/banbox/banbot/factor/statistics"
)

func main() {
    x, y := make([][]float64, 128), make([]float64, 128)
    for i := range y {
        price := 100 + float64(i)/10
        x[i] = []float64{price}
        y[i] = 3 + 1.2*price + math.Sin(float64(i))
    }
    hedge, test, err := statistics.EngleGranger(y, x, true, 1)
    if err != nil { panic(err) }
    mean, scale, err := statistics.MeanStd(hedge.Residuals, 1)
    if err != nil { panic(err) }
    if scale <= 0 { panic("undefined spread scale") }
    // 训练产物冻结后，再读取下一次已发布的价格。
    nextX, nextY := 112.8, 139.0
    spread := nextY - hedge.Intercept - hedge.Beta[0]*nextX
    fmt.Printf("ADF statistic=%.3f, next spread z=%.3f\n", test.TStatistic, (spread-mean)/scale)
}
```

接到运行引擎时，将数据计算注册为 Go definition，或使用现有表达式产生 Frame；
portfolio builder 只消费当期冻结 Frame、Universe 和预算，不读取未来标签。无需把
每个统计函数包进一个统一 facade：回归、在线状态和结算各有不同输入与生命周期，
在真正共享的运行边界复用 runner 即可。

## 普通 YAML 路径

下面是添加到已有交易所、数据库和账户配置的因子配置片段。`momentum-vol` 是内置
示例策略，用于验证普通入口；配对策略需要先在应用中注册自己的 Go definition/builder。

```yaml
data:
  pit_policy: static-approximation
execution:
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
```

```sh
banbot research --config runtime.yml
banbot backtest --mode weights --config runtime.yml
banbot backtest --mode events --config runtime.yml
banbot trade --dry-run --config runtime.yml
```

默认 24 周期示例至少需要 25 条已收盘小时观察才能进入可投资状态。事件执行还需
真实可见 tick 或声明的 1m 价格流、规范化合约单位和账户预算；完整配置以 runner
指南为准。静态 SQL 最新值表无法恢复历史修订和发布时间，必须声明
`static-approximation`；strict PIT 需要不可变 archive 或具有证据的版本页提供者。
回测资金费可以显式声明零费简化；真实永续资金费不能由此推断为零。

Weights 回测的时点宇宙（PIT）按配对判定：某腿在决策网格缺数据（退市、未上市、缺口）时，含该腿的配对平仓并暂停，其余配对继续交易，两腿恢复后可重新开仓；五池相同的静态池同样适用。所有配对同期暂停的周期计入 `Result.PausedPeriods`，不计 `Incomplete`。被平仓腿的退市惩罚由策略条目的 `pit_delist_penalty_bps` 配置（0=默认 100bp，<0 关）。交易所当前市场表已不再列出的永续，在因子存储回放的 Weights 模式下可写入静态 `pairs`：按库内记录解析，条件是库内 K 线范围与回测窗口相交（见 factor_portfolio_guide）；其他模式不支持。

## 每对独立结果与归因

`RegisterPairs` 策略的 Weights 回测在 `Result.Pairs` 给出每对的 `Episodes`（`StartMS/EndMS/Dir/EntryZ/ExitZ/Beta/Bars/Reason/PnL`；`Reason` 为 `exit`、`stop`、`maxhold`、`leg_missing`、`end`，总量上限 400 条在各对间均分（每对至少 10 条），真实数量见 `EpisodeCount`）以及 `PnL`、`Trades`、`Wins`、`WinRate`、`InMarketBars`、`Bars`、`PausedBars`、`AvgHoldBars`、`LastBeta`、`Turnover`。逐对 `PnL`（占初始 NAV 的比例）按该对自身的目标权重（叠加进净权重之前的两项）乘以两腿在相邻决策之间的收益逐期累加，是对账本成交的近似：账本按延迟后的价格成交、含成本、权重在决策之间随价格漂移，且共享品种的各对在账本中按净权重成交。成本不分摊到各对，`Turnover`（`|Δw|` 累计）是成本的代理。对账用 `Result.PairsGrossPnL`（各对之和）、`PairsNAVReturn`（NAV 变化占初始 NAV）与 `PairsResidual`（二者之差，约等于成本加执行近似）。`Result.Attribution` 额外给出多/空腿盈亏、平均敞口、成本合计与 `TopSymbols`，用于识别单一品种贡献过大。

## 时钟与状态的归属

周期决策与不规则 source-event 决策共用 runner，区别只在真实观察如何触发决策。
源事件配置写在 `run_policy[].decision`，不要另建研究命令或伪造周期 tick：

```yaml
decision:
  source_event_decisions: true
  source: information
  timeframe: event
  delay_ms: 0
```

选中源的真实 closed 记录以 EventTime 形成 GridTime，当前批次 publication/receipt
cutoff 形成 DecisionTime；历史回放仍等待 AvailableAt 和 IngestedAt。每轮保持完整
Universe/source 快照屏障。该时钟支持 Research、Weights 和 paper Events，拒绝 live
Trade 和非零 delay。同一 publication 批次出现多个选中源 EventTime 会拒绝；修订
不能重跑已完成的 grid。`run_timeframes` 或 `decision.interval_ms` 仍提供正风险/标签
期限，不能把观察步数的半衰期直接当成小时。完整约束见 runner 的 source-event 小节。

无状态组合函数使用 `RegisterPortfolioBuilder`。有可变过滤器或历史窗口时使用
`RegisterPortfolioFactory(name, factory, validator)`：纯 validator 只校验配置，不能
创建过滤器、借用账户或打开文件。ValidateStatic/Replay/LiveConfig 做预检；
`PreparePortfolioConfig` 在预检后、账户/执行资源装配前为单次运行构造状态。内置运行
路径完成该步骤；嵌入调用者应把返回配置只用于该次运行，不复用到另一轮回测。
构造错误在资源装配前返回；未提供 validator 的旧 factory 在预检阶段只检查注册与身份。

## 训练产物和能力边界

`research.SelectParameters` 的观测必须有真实结束时间、成熟后的 AvailableAt 和成本后
收益；`MinIndependentSamples >= 2`，prior/confidence/HAC lag 非负。它按非重叠 episode
计样本，将稀疏资产建议向组/全局收缩。`ResolveParameters` 校验内容 hash、匹配
manifest、训练结束与发布可见性，测试窗口结果不用于当期选参。模型同样通过
FitModel/PublishModel/LoadModel 保留特征顺序、训练窗口和发布时间。数值合法并不
说明统计显著或参数可盈利；ADF 分布、OU 稳定范围与搜索预算见 statistics 索引。

期权现金结算只在 paper Events 的支持合同内可用；Weights 和 live Trade 不因此
获得期权执行能力。CrossVenuePaper 是独立模拟账本，不是 YAML factor runner 的
跨账户实盘路由。内置 Banexg live 路径针对匹配元数据的线性永续，要求权威资金费现金
能力及 required-stream，且仍需启动账户对账等能力证据。自定义 provider 注册名或配置
开关不能替代这些证据，也不会自动回退到 paper。

测试使用 `go test ./factor/statistics ./factor/research ./factor/runner ./execution`
及相应 `go vet`；这些测试与合成示例验证软件合同。实际成交能力、收益、容量和滑点
需要真实场所验收及独立样本外研究。
