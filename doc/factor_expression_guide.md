# 截面与多因子策略使用指南

截面与多因子策略支持两种定义方式：表达式配置，以及常规 Go builder。两者生成同一种 `factor.Plan`，再由统一 runner 完成组合、持仓目标、回测和实盘决策。底层编译与执行过程见 [核心架构原理](factor_expression_architecture.md)。

直接写公式从第 1 节开始；用 Go 构建和注册策略见第 6 节；两种方式如何运行权重回测、事件回测及实盘见第 7 节；小样本数值和研究检验见第 9 节。

本页使用统一根命令 `backtest`、`trade`、`research`、`validate`、`explain`，归档命令为 `data archive`。策略配置统一使用 YAML。回测和交易按同一 `run_policy` 自动装配时序、因子或混合引擎；因子回测模式依次取 `--mode`、`execution.mode`、默认 `events`，混合回放必须 `events`。本页 `trade --dry-run` 指因子/混合历史回放；纯时序实时模拟继续使用 `env: dry_run`。任务隔离及账户共享约定见[多因子指南](../bandoc/zh-CN/guide/factor.md)。

## 1. 先编写公式，再检查编译

将下面内容保存为 `formula.yml`。这是独立的表达式定义，不包含账户、行情路径或交易设置。

```yaml
schema_version: 1
timeframe: 1h
bindings:
  kline: {source: kline, timeframe: 1h}
params: {window: 24, reversal_window: 3}
lets:
  price: 'positive(kline.close)'
  ret1: 'ts.return(factor.price, 1)'
  momentum: 'ts.return(factor.price, param.window)'
  volatility: 'ts.std(factor.ret1, param.window, 0)'
outputs:
  risk_adjusted: 'cs.zscore(factor.momentum / max(factor.volatility, 1e-8))'
  reversal: 'cs.zscore(-ts.return(factor.price, param.reversal_window))'
combine:
  method: fixed
  weights: {risk_adjusted: 0.7, reversal: 0.3}
```

在 banbot 项目根目录构建并检查；Windows 可将可执行文件名改为 `banbot.exe`：

```sh
go build -o banbot .
./banbot validate --spec formula.yml
./banbot explain --spec formula.yml
```

两条命令当前执行相同的编译检查，输出 JSON，包括 `hash`、`timeframe`、`outputs`、`nodes`、`warmup`、`retention`、`inputs`、`combine`。它们不连接行情、数据库或账户，不能证明字段在真实数据中存在、历史长度足够或数据符合可见性要求。`--spec` 读取独立表达式映射，不读取整个策略配置；文件必须是一个 YAML 文档，大小不超过 1 MiB，未知配置字段会报错。

## 2. 配置字段与名称

| 字段 | 用法 |
|---|---|
| `schema_version` | 必须为 `1` |
| `timeframe` | 决策周期，例如 `1h`、`1d`；策略中须匹配 `run_timeframes` |
| `bindings` | 源别名到真实 `source`、源 `timeframe` 及采样规则的映射 |
| `params` | 有限数值参数，使用 `param.name` 引用 |
| `lets` | 命名中间公式，使用 `factor.name` 引用 |
| `outputs` | 至少一个公开输出，名字用于结果列和组合选择 |
| `combine` | 输出列的组合方法、列选择及权重；由 runner 解析 |

公式中的名称有三类：

```text
kline.close       bindings 中 kline 对应的数据源的 close 字段
factor.momentum   本定义集 lets 或 outputs 中的命名公式
param.window      本定义集 params 中的编译期参数
```

源别名不必与真实源名相同，例如 `prices: {source: kline, timeframe: 1h}` 对应 `prices.close`。自定义数据源和字段同样适用，不要求固定 OHLCV 模型；原始数据继续通过 `orm.DataSeries.Values map[string]any` 提供。字段名包含连字符、空格等字符时写 `field("prices", "adjusted-close")`，参数是两个双引号字符串。

参数名、因子名、绑定别名使用字母或下划线开头，后续可包含数字。绑定别名不能占用 `factor`、`param`、`label`、`ts`、`cs`、`group`。`lets` 和 `outputs` 共享因子名称空间，不能重名；可以前向引用，但不能成环。未使用的 `lets` 不增加最终计算和订阅，仍会接受语法、参数与引用校验。

`factor.name` 只引用当前定义集，不查找其他策略或全局注册的因子。输出建议采用 `momentum`、`risk_adjusted` 等名字；runner 会将组合结果写入 `score`，因此组合场景应避免将原始因子输出命名为 `score`。

## 3. 表达式语法和函数

支持十进制数字、科学计数法、括号、一元 `+`/`-`、四则运算及嵌套函数。`*`/`/` 优先于 `+`/`-`；同级运算左结合，例如 `8 / 4 / 2` 等于 `1`。幂使用 `pow(x,y)`。

### 逐点函数

| 写法 | 语义 |
|---|---|
| `x + y`、`x - y`、`x * y`、`x / y` | 同资产、同时点逐点计算；除法不自动添加 epsilon |
| `positive(x)` | `x > 0` 时保留值，非正数记为 NonFinite |
| `abs(x)` | 绝对值 |
| `log(x)` | 自然对数；非正数产生 NonFinite |
| `sqrt(x)` | 平方根；负数产生 NonFinite |
| `pow(x,y)` | 幂运算，非法结果或溢出产生 NonFinite |
| `min(x,y)`、`max(x,y)` | 两个有效数值的最小值、最大值 |

例如 `log(positive(kline.close)) + log(positive(kline.volume))` 表示对数成交额代理值，可避免先相乘造成的溢出。`max(volatility,1e-8)` 是有效数值的分母下界，不填补 NULL、缺失或预热不足。

### 时序函数

| 写法 | 语义与参数 |
|---|---|
| `ts.lag(x,n)` | 读取过去 `n` 个观察的值；`n=0` 返回原值 |
| `ts.return(x,n)` | `x[t] / x[t-n] - 1` |
| `ts.ema(x,n)` | 沿用现有 banta EMA 初始化和递推口径 |
| `ts.std(x,n,ddof)` | 标准差；必须显式传入 `ddof` |

还支持 18 类常见技术指标、22 个标量函数：`ts.sma/rma/wma/vwma/rsi/roc/mom/tr/atr/cci/stoch/willr/obv/mfi/highest/lowest`，以及 `ts.macd/macd_signal/macd_hist`、`ts.bbands_upper/bbands_middle/bbands_lower`。完整参数表、Go 构造方式、公式、缺失数据与预热规则见[常见技术指标](factor_indicators.md)。全部支持 Batch 与持续推进的 Session。

例如 `ts.cci((kline.high+kline.low+kline.close)/3,20)` 显式构造典型价格，`ts.atr(kline.high,kline.low,kline.close,14)` 提供三个输入，`ts.macd_hist(kline.close,12,26,9)` 选择 MACD 柱。`ts.roc` 返回百分比，`ts.return` 返回比例：100 到 110 分别为 `10` 与 `0.1`。新指标按完整有效元组推进，遇到无效字段不推进状态；原有 `ts.return/lag/ema/std` 保持原合同。

窗口必须为整数常量或已绑定数值参数，范围为 1–10000；`ts.lag` 额外允许 0。`ddof` 必须是整数且满足 `0 <= ddof < n`，其中 `ddof=0` 是总体标准差。窗口参数不接受逐行字段，也不接受 `param.window + 1` 等计算表达式；先在 `params` 中给出最终数值。

先时序再截面是受支持的组合，例如 `cs.zscore(ts.return(kline.close,24))`。当前不支持对截面或回归结果再做时序窗口，即使中间包裹了逐点函数，也会拒绝 `ts.ema(abs(cs.rank(kline.close)),24)`；零 lag 原值例外。

### 截面和回归函数

这些函数在快照的 `Universe.Reference` 上确定统计量，而不是由某个账户的持仓决定统计样本。

| 写法 | 语义与参数 |
|---|---|
| `cs.rank(x)` | 升序、0 起始名次，并列取平均名次；不是百分位 |
| `cs.zscore(x)` | 使用参考池均值和总体标准差标准化；标准差为 0 时有效输出为 0 |
| `cs.winsorize(x,tail)` | 将有效值限制在参考池两端分位点内，`0 <= tail < 0.5` |
| `cs.mad_winsorize(x,k)` | 按参考中位数与缩放 MAD 截尾，k 必须为正 |
| `cs.robust_zscore(x)` | 中位数 / `1.4826 × MAD` 稳健标准化，零 MAD 输出零 |
| `cs.quantile(x,q)` | 参考池分位数，`0 <= q <= 1`；同一分位值广播给有有效输入的资产 |
| `group.residual(y,x)` | 用参考池完整有效配对拟合带截距的一元截面回归，输出残差 |
| `group.ols(y,x1,x2,...)` | 含截距的多暴露回归残差，共线暴露按声明顺序处理 |
| `group.wls(y,weight,x1,x2,...)` | 正权重的多暴露加权回归残差 |
| `group.demean(x,"source","field")` | 按原始分组字段计算组内去均值 |
| `group.zscore(x,"source","field")` | 按原始分组字段计算组内标准化 |

分位点使用排序值间的线性插值。例如 `[10,10,30]` 的 rank 为 `[0.5,0.5,2]`，中位数为 `10`。三项全相同时 rank 均为 `1`，z-score 均为 `0`。

`group.residual` 沿用内核的有效性规则：`x` 无效的观测记为 Missing，完整配对不足时其他有效目标可能得到 Warmup。需要精确保留回归输入原始无效原因的策略，可以继续使用有明确依赖和版本的 Go 定义。分组函数的 source 须在 bindings 中声明，字段保留原始类型/NULL；慢频暴露应使用显式 asof 和 max_age_ms。统计量只在当时冻结的 Reference 拟合。详细数值合同见 [研究扩展](factor_research_extensions.md)。

## 4. 多因子组合

`outputs` 决定输出哪些因子列；`combine` 决定如何将这些列合成组合分数。默认方法是 `equal`，默认列为全部输出。

```yaml
combine:
  method: fixed
  columns: [risk_adjusted, reversal]
  weights: {risk_adjusted: 0.7, reversal: 0.3}
```

`fixed` 按给定权重直接相加，允许负权重，不自动归一化。每个选中列都必须有有限权重，权重不能指向未选中列；列名必须存在且不重复。非零权重列无效会使对应资产的组合分数无效，不会针对该资产临时重分配权重；零权重列不参与求值有效性判断。外层 `combo` 如显式设置 `method`，会覆盖表达式内的整个组合配置。

历史方法包括 `history-ic`、`history-rank-ic`、`history-icir`、`history-rank-icir`、`history-ewma`，只使用已成熟且在决策时可见的样本。通过 `label` 选择期限，`min_samples/min_pairs/min_confidence` 控制门槛，`direction` 控制方向，`fallback` 显式选择 equal/fixed/error；EWMA 的 decay 为 alpha。单标签省略 label 保持原身份，多标签省略时选择最短期限，同期限按名称稳定排序。当前实盘驱动拒绝全部历史方法，需接入成熟历史 provider 后才能启用。未来标签不进入公式，不能写 `label.future_return`。

## 5. 放入策略配置并运行

下面是使用本地不可变归档的权重回测示例，保存为 `strategy.yml`。`data.gob` 需要包含所需资产的小时 K 线，并覆盖预热与研究标签区间。

```yaml
wallet_amounts: {USD: 10000}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: MyMomentum
    id: my_momentum
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    archive: data.gob
    prices: {source: kline, timeframe: 1h, field: close}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      outputs:
        momentum: 'cs.zscore(ts.return(positive(kline.close), param.window))'
      combine: {method: equal}
```

在表达式模式下，`run_policy.name` 是策略名称，无需注册同名 Go definition。不要同时指定 `definition`；Go API 中的 `Config.Expressions` 也不能与 `Config.Plan` 或非空 `Config.Definition` 一起使用。策略内省略表达式 `timeframe` 时会继承决策时间周期；独立 `formula.yml` 和 Go `expr.Spec` 则必须自行提供。

外层 `run_policy.params.k` 控制选股数量；`expressions.params.window` 控制公式窗口，两者不会自动相互复制。

```sh
./banbot research --config strategy.yml
./banbot backtest --mode weights --config strategy.yml
./banbot backtest --config strategy.yml
```

`research` 使用统一驱动和成熟标签生成因子研究结果。普通配置默认提供一个决策周期的 executable-return 标签；可通过 `research.labels` 同时声明多个期限，共享冻结 Frame 并分别捕获价格、成熟和报告 unresolved，例如：

```yaml
research:
  labels:
    - name: forward_1h
      kind: executable-return
      horizon: 3600000
      periods_per_year: 8766
      overlapping: true
    - name: forward_16h
      kind: executable-return
      horizon: 57600000
      periods_per_year: 547.5
      overlapping: true
```

这段映射直接放在 `run_policy[]` 的策略条目下。各期限分别占用有界 max_pending；长周期可能在回放尾端未成熟，未完成结果不能当零收益。纯交易回放使用固定/等权且不需要研究时，可显式设置 `research: {labels: []}`；`research` 和全部历史合成方法不能关闭标签。手工 Go 研究可另用 `research.ReturnLabel`、`LabelQueue`、`Evaluate`，并遵守各自标签的成熟和可见时间。

从普通历史数据库读取时，使用已有数据库、市场、交易对池和 `time_range` 基础配置，移除 `archive`，声明 `data.pit_policy: static-approximation`。普通最新值存储不能证明历史修订的严格 PIT；需要严格 PIT 时使用具有可见性和版本记录的不可变归档或受验证的历史输入。

成交价格 `prices` 独立于因子输入。资金费率、财务字段等不能作为隐式成交价格；归档模式未显式指定时只尝试已声明的通用 tick/kline 价格源。`events` 和真实交易还要求 tick/event 或 1m 可观察价格、执行单位及账户绑定。使用 `backtest --mode events` 或 `trade` 前须完成相应执行配置，单独一份公式不能提供这些资源。`funding_policy: explicit-zero` 是明确忽略资金费率的假设；需要真实资金费用时声明所需 funding 流。

## 6. 用常规 Go 代码构建策略

### 原生节点构建单因子和多因子

Go builder 的基本链路是 `Field → 时序节点 → 截面节点 → 命名输出 → Compile`。组合规则由独立的 `research.ComboSpec` 提供；例如动量和低波各自做截面 z-score，再按 0.7/0.3 合成。

下面是可构建的独立入口程序，可以保存为自己策略项目的 `cmd/factorbot/main.go`。它注册单因子 `CodeMomentumV1` 和多因子 `CodeMultiFactorV1`，然后运行 banbot 的命令入口。与表达式不同，新增或修改这段 Go 代码需要重新编译程序。

```go
package main

import (
    "fmt"
    "math"

    "github.com/banbox/banbot/entry"
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/research"
    "github.com/banbox/banbot/factor/runner"
)

func buildCodeFactors(c runner.Config, multi bool) (*factor.Plan, research.ComboSpec, error) {
    window := 24
    for name, value := range c.Manifest.Parameters {
        if name == "k" { continue } // 持仓选择由入口和 portfolio 负责。
        if name != "window" {
            return nil, research.ComboSpec{}, fmt.Errorf("unknown parameter %q", name)
        }
        if math.IsNaN(value) || math.IsInf(value, 0) ||
            value < 2 || value > 10000 || value != math.Trunc(value) {
            return nil, research.ComboSpec{}, fmt.Errorf("window must be an integer in [2,10000]")
        }
        window = int(value)
    }
    if c.Factor.Source == "" || c.Factor.Field == "" || c.Factor.TimeFrame == "" {
        return nil, research.ComboSpec{}, fmt.Errorf("source, field and timeframe are required")
    }
    price := factor.Positive(factor.Field(c.Factor.Source, c.Factor.Field, c.Factor.TimeFrame))
    momentum := factor.Return(price, window)
    builder := factor.New().Add("momentum", factor.ZScore(momentum))
    combo := research.ComboSpec{Method: research.Equal, Columns: []string{"momentum"}}
    if multi {
        volatility := factor.StdDev(factor.Return(price, 1), window, 0)
        builder.Add("low_volatility", factor.ZScore(factor.Neg(volatility)))
        combo = research.ComboSpec{
            Method: research.Fixed,
            Columns: []string{"momentum", "low_volatility"},
            Weights: map[string]float64{"momentum": 0.7, "low_volatility": 0.3},
        }
    }
    plan, err := builder.Compile()
    return plan, combo, err
}

func init() {
    if err := runner.RegisterDefinition("CodeMomentumV1", func(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
        return buildCodeFactors(c, false)
    }); err != nil { panic(err) }
    if err := runner.RegisterDefinition("CodeMultiFactorV1", func(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
        return buildCodeFactors(c, true)
    }); err != nil { panic(err) }
}

func main() { entry.RunCmd() }
```

`RegisterDefinition` 注册的是完整计划构建器，重名或空构建器会失败。代码中的两个 definition 有独立名称，原策略可以继续并存。将定义放在独立 Go 包时，在主程序中导入该包以触发 `init`；仅创建文件而没有编入当前可执行程序，会得到 `unregistered definition`。

注册后，YAML 不需要 `expressions`；策略参数通过入口进入 `c.Manifest.Parameters`。以下归档配置与第 5 节使用相同的价格和持仓口径：

```yaml
wallet_amounts: {USD: 10000}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: CodeMultiFactorV1
    id: code_multi
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 3}
    archive: data.gob
    prices: {source: kline, timeframe: 1h, field: close}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
```

保存为 `code_strategy.yml`；将 `name` 改为 `CodeMomentumV1` 可选择单因子版本。也可以用 `definition` 明确指定 Go definition，让 `run_policy.name` 使用其他策略展示名。

```sh
go build -o factorbot ./cmd/factorbot
./factorbot research --config code_strategy.yml
./factorbot backtest --mode weights --config code_strategy.yml
./factorbot backtest --config code_strategy.yml
```

`c.Factor` 由入口提供通用 kline/close 和决策周期默认值；在 Go API 中直接调用构建器时，须自己提供这些值和参数。可以通过 `runner.CompileDefinition` 只解析注册计划，或者将已经构建的 `Plan`、完整 `ComboSpec` 传入 `runner.Config`；直接传 Plan 时组合不会从注册 definition 自动生成。正常代码使用方式应选择一个明确入口；表达式模式与显式 Plan/definition 互斥。

### 自定义计算和持仓构建

原生 Go builder 支持表达式白名单以外的节点，例如 `GroupDemean`、`GroupZScore`；其源周期、分类字段和可见性契约仍需由调用方明确提供，不能推断任意混合周期已经可用。新公式优先组合现有原生 `Add/Sub/Mul/Div/Neg/Abs/Log/Sqrt/Pow/Min/Max` 等节点，预热和公共子图仍由 Compile 推导。

只有缺少原子计算时，才使用 `factor.Custom(version, inputs, evaluate)` 编写可信纯逐点函数。`inputs` 声明依赖，回调消费 `[]factor.Numeric` 并返回 Numeric；必须保留无效原因，不能偷偷读取账户、最新行情或修改共享状态。不同实现必须使用不同版本，编译器不会自动识别闭包代码。涉及历史状态的新算子需要内核支持和 Session/Batch、克隆与恢复测试，不能在 Custom 闭包中自建隐式滚动状态。

因子和组合分数确定后，默认持仓构建器按 `score` 选择最高/最低各 `k` 个有效、可投资且可交易资产，根据 `long_notional`、`short_notional` 分配冻结 NAV 的名义权重。至少需要 `2*k` 个有效候选；不足或全部分数相同时跳过替换并保持已有组合，同时输出诊断。它输出的是目标组合，不是策略逐资产自行下单。

需要行业约束、风险预算或其他持仓规则时，可使用 `runner.RegisterPortfolioBuilder` 注册独立版本名，并在 `portfolio.builder` 中指定。其函数签名见 [definition.go](../factor/runner/definition.go)：消费冻结 Frame、Universe、PortfolioSpec 和 PortfolioDefinition，返回 TargetPortfolio、诊断和错误。也可在 Go Config 中传入 `PortfolioBuilder`，同时提供 manifest 的版本名。组合与账户状态应保持在各自运行实例中，不放进共享因子计算状态。

builder 产生理想权重；持仓年龄、调仓日程、退出曲线和批次使用 `PortfolioPolicy`。通过 `portfolio.policy: lifecycle-v1` 启用常见预设，或以带版本的工厂注册完整自定义 policy。最终 `PortfolioTarget` 可混合 NAV 权重和精确资产数量；旧 `TargetPortfolio` 保持纯权重。配置、恢复和自定义方法见 [组合与持仓指南](factor_portfolio_guide.md)。纯 CLI research 没有实际持仓证据源，不能直接启用有状态 policy；研究交易期限可先用 weights 参数扫描，再调用成本/生命周期报告 API。

## 7. 两种定义方式的回测与实盘

Go 和表达式只影响计划来源，后续命令、行情、执行和账户装配相同。下面用 `factorbot` 代表已编入所需 Go 定义及运行集成的程序；表达式方式可以使用普通 banbot 程序。

| 模式 | 命令 | 主要用途和输入条件 |
|---|---|---|
| 因子研究 | `research --config strategy.yml` | 研究覆盖率、IC/RankIC 等，必须有成熟标签及所需观察价格 |
| 权重回测 | `backtest --mode weights --config strategy.yml` | 按目标权重和成本口径回放，用于快速比较组合 |
| 事件回测 | `backtest --mode events --config strategy.yml` | 通过账户账本与模拟执行器处理订单/成交，需要可观察执行价格及合约单位 |
| 普通回测入口 | `backtest --config strategy.yml` | 使用统一策略配置，执行模式由配置/普通回测装配决定 |
| 本地模拟回放 | `trade --dry-run --config strategy.yml` | 当前实现转换为 events 历史回放，不是连接实时行情的 paper trading |
| 实盘 | `trade --config strategy.yml --live-provider binding-name` | 使用已注册、通过能力验证的实盘绑定及真实账户 |

### 数据库回测和事件执行

归档示例可直接使用 `archive`，无需数据库。如果使用已有行情数据库，将以下内容保存为策略覆盖配置，并与完整基础配置一起加载：

```yaml
data: {pit_policy: static-approximation}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: CodeMultiFactorV1
    id: code_multi
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 3}
    prices: {source: kline, timeframe: 1m, field: close}
    decision: {latency_ms: 1, expiry_ms: 120000}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
```

```sh
./factorbot backtest --config base.yml --config code_storage.yml
./factorbot backtest --mode events --config base.yml --config code_storage.yml
```

`base.yml` 必须提供数据库、市场、账户、交易对池和 `time_range`。表达式策略则在此覆盖配置中换成对应 `expressions`，并移除 Go definition 选择。事件执行还需要可验证的 `execution.instruments`、保证金和账户/策略风险限制；普通存储装配可以从已验证市场信息补齐支持的单位，归档需要明确提供完整元数据。执行配置字段见 [advanced.go](../config/advanced.go)，校验条件见 [validate.go](../factor/runner/validate.go) 和 [factor_storage.go](../entry/factor_storage.go)。

小时/日线可作为因子输入，但 events 使用独立 tick/event 或 1m 价格流。仅有日 K 的研究归档不能充当真实事件撮合数据。费用、资金费率、延迟和有效期应在比较两种定义方式时保持相同；单因子 IC 也不能代替费用后的组合回测收益。

### 实盘集成与运行

实盘沿用同一 Plan/ComboSpec，通过实盘回合屏障等待所需资产和源的数据就绪，构建冻结快照并生成目标组合。表达式或 Go definition 本身不负责连接交易所。

实盘入口默认提供 `banexg` binding，消费 SDK 的统一执行和资金费能力；当前支持生产环境的线性永续、单向净持仓。自定义账户/数据集成仍可通过 `entry.RegisterFactorLiveBinding(name, factory)` 注册，签名和返回结构见 [factor_live.go](../entry/factor_live.go)。交易所适配继续由 banexg 提供统一接口，策略不编写交易所独有分支。

配置、资金分配、订单恢复和小额实盘验收见 [factor_live_trading.md](factor_live_trading.md)。`ValidateLiveConfig` 是纯配置检查，不能替代 transport、账户快照和恢复能力的运行验证；SDK 能力缺失或账户核对失败时禁止启动交易。

完成这些集成后，在包含市场/账户、资金费率政策及独立报价配置的 `live_base.yml` 上叠加策略：

```sh
./factorbot trade --config live_base.yml --config code_live.yml --live-provider my_verified_binding
```

`code_live.yml` 可沿用上面的 Go 选择或表达式配置，但须移除 `archive`，使用实时行情源，并提供或由已验证绑定补齐 Universe、SID 映射、schema 和 source version。多策略共用账户时明确 `capital_weight` 和账户/策略风险限制；预算来自对账后的账户状态。实盘使用 `equal`/`fixed`，当前驱动拒绝 `history-ic`。

在自己维护行情与账户生命周期的 Go 应用中，可以调用 `runner.NewLive(c, sink, clock, output)`，启动时通过 `Warmup` 提交完整历史、用 `ValidateWarmup` 确认预热到目标网格，再用 `Observe` 提交带版本/可见时间的实时记录、`Flush` 推进决策、`Stop` 释放资源。历史驱动对应 `runner.Run(ctx,c,sink,output)`。这些是装配 API：仍须提供有效 Config、必要的预算/执行 sink 和输入，不是将 Plan 传进去就可以自动获得账户或行情。

## 8. 多源与显式采样

源周期与决策周期相同且按当前事件取值时，`sampling` 可省略。不同周期或 event 源必须显式声明 asof 和正的最大年龄，例如：

```yaml
schema_version: 1
timeframe: 1h
bindings:
  kline: {source: kline, timeframe: 1h}
  funding:
    source: funding
    timeframe: event
    sampling: asof
    max_age_ms: 28800000
outputs:
  momentum: 'cs.zscore(ts.return(positive(kline.close), 24))'
  carry: 'cs.zscore(-funding.rate)'
combine:
  method: fixed
  weights: {momentum: 0.8, carry: 0.2}
```

`sampling: asof` 与 `asof-latest` 等效。它在决策网格取当时可见且未超过 `max_age_ms` 的观测；发布、接收、事件时间与屏障仍由数据输入和快照负责。超龄或缺少所需流会阻止求值，不会自动读取未来观测。

asof 后的时序窗口计数为决策观察次数。在小时决策上重复采样日频数据后计算 24 个观察的窗口，不等于 24 个交易日。需要原生日频指标时，先在日频数据源生成指标，再显式采样该输出。

## 9. 检查数值、有效性和因子效果

有效性区分 Valid、Missing、Null、NotNumeric、NonFinite、Warmup。缺字段、显式 `nil`、字符串、溢出和历史不足都不填零。逐点函数按左到右传播第一个无效输入；时序/截面/回归函数沿用各自窗口和样本规则。

新增公式推荐按以下顺序验证：

1. `validate --spec` 检查名称、函数、窗口、采样、组合和依赖环。
2. 使用少量资产和短历史手算，检查每个时点及预热、NULL、缺字段、非法数值和恢复路径。
3. 用相同快照比较 Session 与 Batch；历史分块时保持同一个 Session 连续推进，避免重复初始化 EMA。
4. 使用成熟标签研究覆盖率、IC/RankIC、因子相关性和稳定性，再按相同费用与价格口径做交易回测。

下面是完整的 Go 小样本检查，可保存为 `factor_demo.go`，在 banbot 模块根目录执行 `go run /path/to/factor_demo.go`。它验证三资产收益 `[0,0.1,0.2]` 的总体 z-score 为 `[-sqrt(1.5),0,sqrt(1.5)]`。

```go
package main

import (
    "fmt"
    "math"

    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/expr"
    "github.com/banbox/banbot/orm"
)

func main() {
    plan, err := expr.Compile(expr.Spec{
        SchemaVersion: 1, TimeFrame: "1h",
        Bindings: map[string]expr.Binding{
            "kline": {Source: "kline", TimeFrame: "1h"},
        },
        Outputs: map[string]string{
            "momentum": "cs.zscore(ts.return(positive(kline.close),1))",
        },
    })
    if err != nil { panic(err) }
    session, err := factor.NewSession(plan)
    if err != nil { panic(err) }
    sids := []int32{1, 2, 3}
    var frame factor.Frame
    for bar := 0; bar < 2; bar++ {
        at := int64(bar+1) * 3600000
        var rows []factor.VersionRecord
        var requirements []factor.Requirement
        for _, sid := range sids {
            price := 100.0 + float64(bar)*10*float64(sid-1)
            rows = append(rows, factor.Record(orm.DataSeries{
                Source: "kline", Sid: sid, TimeMS: at-3600000,
                EndMS: at, TimeFrame: "1h", Closed: true,
                Values: map[string]any{"close": price, "nullable": nil},
            }, 1, at, at, "v1"))
            requirements = append(requirements, factor.Requirement{
                SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at,
            })
        }
        snapshot, err := factor.Freeze(factor.SnapshotSpec{
            DecisionTime: at, ReplayTime: at,
            Universe: factor.Universe{
                Version: "demo", Investable: sids, Reference: sids,
                Tradable: sids, Evaluation: sids, Static: true,
            },
            SIDMap: map[int32]string{1: "A", 2: "B", 3: "C"},
            Schemas: map[string]string{"kline": "demo-schema"},
            SourceVersions: map[string]string{"kline": "v1"},
            VisibilityPolicy: "published-and-received",
        }, rows, requirements)
        if err != nil { panic(err) }
        frame, err = session.Evaluate(snapshot)
        if err != nil { panic(err) }
    }
    for _, sid := range sids {
        got := frame.Values["momentum"][sid]
        want := float64(sid-2) * math.Sqrt(1.5)
        if got.Validity != factor.Valid || math.Abs(got.Value-want) > 1e-10 {
            panic(fmt.Sprintf("SID %d: got %+v, want %g", sid, got, want))
        }
    }
    fmt.Println("factor check passed")
}
```

`expr.Compile` 返回计划，组合应由 `runner.CompileDefinition(runner.Config{Expressions: &spec})` 解析，或单独调用 `research.Combine` 并提供完整 ComboSpec。直接运行 Session 不会自动读取 `Spec.Combine`，也不会下单。

Go 代码版采用相同的小样本快照和 Session 检查：将示例中的表达式编译替换为 `factor.New().Add("momentum", factor.ZScore(factor.Return(factor.Positive(factor.Field("kline", "close", "1h")), 1))).Compile()`，其余数据、断言都保持一致。测试已注册定义时，通过 `runner.CompileDefinition` 提供 `Definition`、`Factor` 和 `Manifest.Parameters` 获取 Plan/ComboSpec；逐 bar 对照因子列，再用 `research.Combine` 对照组合分数。这样可以分别检查计算、组合和 runner 运行行为，避免只用最终收益证明实现正确。

与当前实现配套的测试：

```sh
go test ./factor/... ./entry -run 'Expression|Arithmetic|Pointwise|Constant|SessionBatch' -count=1
```

完整的编译、手算、CSE、错误边界测试见 [compile_test.go](../factor/expr/compile_test.go)；原生算子测试见 [pointwise_test.go](../factor/pointwise_test.go)；归档/实盘决策一致性见 [expressions_test.go](../factor/runner/expressions_test.go)；配置和命令示例见 [factor_expressions_test.go](../entry/factor_expressions_test.go)。

如果本地同时有 `../banstrats`，其 `examples/crosssection` 保留了原 Go 策略，并在独立 `expressions.go` 中提供六个 `Expr` 后缀版本。可在 banstrats 根目录运行：

```sh
go test ./examples/crosssection/... -count=1
go test ./examples/crosssection -run '^$' -bench '^BenchmarkFactorVersions$' -benchmem -benchtime=500ms -count=3 -cpu=1
```

对应的 `expressions_test.go` 逐 bar 对照原版，`performance_test.go` 对照同一份 100 资产 × 256 根日线的 Compile、Session、Batch。`BENCHMARK_expressions.md` 和 `benchmark_versions.txt` 保存实测口径及原始结果。表达式降低编写成本，但拆分原生节点可能增加运行成本，应分别测量编译和执行，不能仅凭公式等价宣称更快。

研究筛选时保存公式、参数、数据版本、资产池、标签和费用口径。标准化/填补等学习步骤仅在训练段拟合，标签跨训练边界时剔除重叠样本，验证后使用独立测试段；大量尝试中最高的样本内 IC 不代表稳定交易收益。

## 10. 常见错误和当前边界

| 问题 | 检查方式 |
|---|---|
| `unknown binding/factor/parameter` | 核对命名空间、声明及拼写；裸 `close` 不等于 `kline.close` |
| `unknown function` / 参数数量错误 | 使用上面的函数表；`ts.std` 必须提供第三个参数 |
| 窗口或 `ddof` 超界 | 使用编译期整数参数；`ts.return(...,0)` 无效，零 lag 有效 |
| `cyclic factor reference` | 将相互引用的 lets/outputs 改为无环依赖 |
| `TS windows over cross-section results` | 先做时序窗口，再做截面变换 |
| 周期不匹配或缺少 asof | 同周期源用当前事件，不同周期/event 源显式配置采样和正最大年龄 |
| 组合列或权重错误 | 核对 outputs、columns、weights，以及外层 combo 覆盖 |
| 源码位置错误 | `outputs.name:行:列` 指公式字符串中的位置，不是 YAML 文件物理行号 |
| 编译成功但结果无效 | 核对原始字段/类型、预热、有效样本、源可见性和年龄；不要统一填零 |
| 有分数却没有新目标 | 查看不足 `2*k` 个有效候选、分数全相同、不可交易资产等组合诊断 |

资源限制为单公式 16 KiB、全部公式文本 256 KiB、AST 节点 8192、解析/展开深度预算 64；`lets + outputs`、bindings 和 params 的声明数量分别不超过 512。解析器对内部递归层数也计数，因此嵌套括号/函数未必能达到 64 个。限制是编译边界，不是对任意资产数量下内存使用的保证。

当前没有赋值、比较、逻辑条件、三元/`where`、自动填补、数组索引、注释或任意 Go/Python 调用；不支持 `^`、十六进制浮点及数字下划线。也没有跨定义库引用、自动候选生成或 `factor operators/generate/test` 命令。需要新的原子算法时按照架构文档中的扩展步骤修改 Go 内核，并补充独立数值和 Session/Batch 测试。

## 2026-10-04 双引擎使用入口

run_policy.engine 接受 time_series/factor，省略时为时序。原生多因子图、表达式、PIT、成熟标签、weights/events、混合账户和实时生命周期见[多因子与截面指南](../bandoc/zh-CN/guide/factor.md)及[API](../bandoc/zh-CN/api/factor.md)。逐包结论和本次验证见[重构记录](strategy_engine_refactor.md)。

execution.live_provider: verified-session 只是用户工厂示例名，必须先注册 entry.RegisterFactorLiveBinding("verified-session", factory) 并提供真实证据。内置 empty/banexg 或未注册工厂缺能力时明确失败，不自动降级 paper；trade --dry-run 是历史模拟。最新值数据库必须显式 static-approximation；任意字段/NULL 继续通过 DataSeries.Values。
