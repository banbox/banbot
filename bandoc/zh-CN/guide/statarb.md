# 统计套利与期现

统计套利策略运行在因子引擎（`engine: factor`）上，通过专用注册接口声明配对、篮子或残差模型，由运行器完成滚动估计、状态机、目标组合和账本。品种与结构写在 Go 代码里，阈值和窗口写在 YAML `params` 中。通用的 `run_policy`、价格流、组合和结果约定见[多因子与截面策略](./factor.md)。

| 需求 | 注册接口 | 说明 |
| --- | --- | --- |
| 固定的两腿价差回归 | `runner.RegisterPairs` | 品种写在 Go 代码里 |
| 从候选池周期性重选配对 | `runner.RegisterRollingPairs` | 品种来自 YAML `pairs` |
| 2 到 12 腿协整篮子 | `runner.RegisterBaskets` / `RegisterBasketsParams` | Johansen 或多元 OLS 权重 |
| 截面 PCA / 基准残差 s-score | `runner.RegisterResidualStatArb` / `RegisterResidualStatArbParams` | 美元中性且因子 beta 中性 |
| 现货加永续的基差回归（carry） | `RegisterPairs` + `SpreadModel: "ratio"` | 需要 `extra_markets: [spot]` |

可运行的示例位于 banstrats 仓库：[examples/statarb](https://github.com/banbox/banstrats/tree/main/examples/statarb) 与 [examples/carry](https://github.com/banbox/banstrats/tree/main/examples/carry)。示例阈值只是演示起点，没有经过优化，也不代表收益。

## 运行前提

- 用 `bot backtest`（`execution.mode: weights`）查看绩效；`bot research` 对配对和篮子只输出对数价格面板与标签诊断。
- 决策周期默认 `1h`，需要池内全部标的的 K 线。成交价流应使用 `prices: {source: kline, timeframe: 1m, field: close}`，并把 `decision.expiry_ms` 设为 120000；只用 1h 收盘价流会让全部目标过期、零成交，原因见[零成交陷阱](./factor.md#零成交陷阱)。
- 窗口都按决策 K 线根数计数。改 `run_timeframes`（支持 4h、8h、3d 等聚合周期）时同步调整 `window`、`formation`、`reselect`。
- `portfolio.mode` 保持 `full`：平仓的配对会从目标中删除，`patch` 模式不会平掉未列出的仓位。
- 必须显式声明 `funding_policy`。`explicit-zero` 把资金费按零处理，会低估永续配对的成本；需要真实资金费时使用 `storage` 或 `required-stream`。

覆盖文件示例（与你自己的数据库、交易所、`time_range` 基础配置一起加载）：

```yaml
pairs: ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
data:
  pit_policy: static-approximation
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: SAPairsEthBtc
    engine: factor
    run_timeframes: [1h]
    params: {window: 120, entry: 2, exit: 0.5, stop: 4, gross: 0.5}
    prices: {source: kline, timeframe: 1m, field: close}
    decision: {latency_ms: 1, expiry_ms: 120000}
    portfolio: {mode: full}
    manifest:
      costs: {fee_rate: 0.0005, slippage_rate: 0.0005, funding_policy: explicit-zero}
    research: {labels: []}
```

```sh
./bot backtest --config your-runtime.yml --config examples/statarb/pairs.yml
```

## 固定配对：RegisterPairs

```go
func init() {
	if err := runner.RegisterPairs("SAPairsEthBtc", func(p *runner.Params) (runner.PairsSpec, error) {
		return runner.PairsSpec{
			Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
			Window: p.Int("window", 120),
			Band: statistics.BandConfig{
				Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
				Stop: p.Float("stop", 4), MaxHold: p.Int("maxHold", 0),
			},
			Gross: p.Float("gross", 0),
		}, p.Err()
	}); err != nil {
		panic(err)
	}
}
```

`Pair{Y, X}` 的符号须与运行时品种完全一致。`build` 在一次运行中会被多次调用，应是无副作用的纯函数；`Params` 没有字符串读取方法，因此品种不能由 YAML 参数化。拼错的参数名不会自动报错，需在返回前检查 `p.Unused()`。注册后 YAML 只需写 `name`，运行器自动绑定对应的组合构造器。

### 信号与仓位

每个决策 bar，对最近 `Window` 个有效观测拟合价差，`z = 残差 / 残差标准差`：

- `z <= -Entry` 做多价差（买 Y 卖 X），`z >= Entry` 做空价差，`|z| <= Exit` 止盈平仓。
- `|z| >= Stop`（可选）止损；`MaxHold`（可选，决策 bar 数）到期平仓。止损或到期后，z 回到 `[-Exit, Exit]` 内才允许再入场。
- `BandConfig.Mode` 取 `reversion`（默认，均值回归）或 `momentum`（顺势突破）；`LongOnly`/`ShortOnly` 限制方向。校验要求 `Entry > 0`、`0 <= Exit < Entry`，均值回归模式下 `Stop` 为 0 或大于 `Entry`。阈值没有内置默认值。
- 每对总名义为 `Gross`（占策略 NAV，默认 `1/配对数`，合计不得超过 1）。Y 腿权重 `s·G/(1+|β|)`，X 腿 `-s·G·β/(1+|β|)`。同一品种出现在多对时权重叠加。

### 对冲比与退出 z

- **正 β 守卫**：默认拒绝入场时 `β <= 0` 的配对（避免两腿同向）；`MinBeta` 设置下限，`AllowNegativeBeta: true` 允许负斜率。
- **冻结退出 z**：持仓期间对冲比固定为入场时的值，退出与止损 z 用该冻结 β 在当前窗口上计算，避免滚动 β 漂移造成虚假回归；`DynamicExitZ: true` 改用每轮重拟合的 z，`DynamicHedge: true` 让持仓对冲比随在线估计更新。
- **漂移缓冲**：`DriftBand`（L1 权重距离）。持仓期权重自然漂移小于该值时保持现有仓位，减少无谓换手；为 0 时每轮都再平衡回目标权重并计费。
- **逆波动率配比**：`WeightMode: "inverse_vol"` 把总敞口 `Gross × 配对数` 按各持仓配对残差标准差的倒数分配，单对上限 `MaxPairGross`（默认 `min(总敞口, 2×Gross)`）。

### 价差模型 SpreadModel

| 取值 | 含义 |
| --- | --- |
| `""` / `log_ols`（默认） | 对数价格滚动 OLS，β 为弹性 |
| `raw_ols` | 原始价格滚动 OLS，自动换算美元名义对冲比 |
| `tls` | 对数价格正交回归，交换 Y/X 结果对称 |
| `kalman` | 随机游走状态空间（`KalmanDelta`、`KalmanVe`，0 取默认），以更新前预测新息作无前视 z |
| `ratio` | 价格比值 `Y/X` 的滚动布林 z，固定等额美元对冲（β=1） |
| `return_diff` | 窗口累计收益率差的 z，固定等额美元对冲 |
| `copula` | 秩变换后的条件概率入场；`Copula.Family` 取 `auto`（默认）、`empirical`、`gaussian`、`clayton`、`gumbel`、`frank`，`LowerProb`（默认 0.05）、`UpperProb`（默认 0.95）、`ExitBand`、`StopProb` |

### 入场闸门与过滤钩子

下列条件只拦截新开仓，已有持仓的退出与止损不受影响：

| 字段 | 作用 |
| --- | --- |
| `MinHalfLife` / `MaxHalfLife` | 对价差窗口做 `FitOU`，离散半衰期（bar 数）须在范围内；不回归的价差不开仓 |
| `MinRollingCorr` / `CorrWindow` | 1-bar 对数收益相关系数下限 |
| `MinHurst` / `MaxHurst` | 价差窗口 Hurst 范围（均值回归常设 `MaxHurst: 0.5`） |
| `SpreadRSIPeriod` / `SpreadRSIUpper` / `SpreadRSILower` | 价差 Wilder RSI 闸门，默认 70/30 |
| `FeatureExprs` / `ExtraFeatures` | 为每条腿生成特征列（表达式或因子节点，绑定别名 `kline`） |
| `EntryFilter` | `func(PairFilterContext) bool`，在入场时读取当期无前视特征（`FeatureY/FeatureX`），返回 false 则拦截 |

banstrats 的 [pairs.go](https://github.com/banbox/banstrats/blob/main/examples/statarb/pairs.go) 展示了用 `FeatureExprs` 加 `EntryFilter` 在任一腿波动率飙升时拒绝开仓。这些闸门配合 `BandMode`、Kalman 与 copula 模型的可选参数，都由 `params` 控制，改动任何参数即产生新的策略身份。

### 缺数据处理

窗口未满时不生成信号；已开配对的任一腿缺失时请求平仓并暂停，数据恢复后可重新入场。回测窗口内完全没有数据的腿（退市、未上市）会让含该腿的配对整次跳过，其余配对照常运行且不会放大预算；全部配对不可用时回测正常结束，零交易、权益不变。拼写错误或不在运行品种池内的腿仍会报错。

## 滚动重选：RegisterRollingPairs

不写任何品种字面量，以 YAML 根配置 `pairs` 为候选池，按“形成期 + 交易期”周期性重选配对，每次只用已收盘数据：

```go
runner.RegisterRollingPairs("SARollingPairs", func(p *runner.Params) (runner.RollingPairsSpec, error) {
	return runner.RollingPairsSpec{
		FormationBars: p.Int("formation", 240),
		ReselectBars:  p.Int("reselect", 120),
		PairCount:     p.Int("pairCount", 4),
		MaxPerSymbol:  p.Int("maxPerSymbol", 2),
		Window:        p.Int("window", 120),
		WeightMode:    "inverse_vol",
		Band: statistics.BandConfig{
			Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
			Stop: p.Float("stop", 4), MaxHold: p.Int("maxHold", 60),
		},
	}, p.Err()
})
```

- `SelectionMethod`：`cointegration`（默认，Engle-Granger 残差 ADF `<= MaxADF`，缺省 -2.86，加半衰期范围）或 `distance`（Gatev 最小距离法）。另有覆盖率 `MinCoverage`、相关 `MinCorr` 要求。
- 每个重选点按得分升序贪心选出至多 `PairCount` 对，同一品种至多出现 `MaxPerSymbol` 次，并列按符号确定性打破，同输入结果逐位一致。
- 首次满 `FormationBars` 之前没有目标；新入选配对用此前可见历史预热，当轮即可交易。
- `HandoffMode`：`graceful`（默认）让未连任的持仓配对只退不进直到正常出场，`immediate` 在换期当根平仓。
- 在线闸门（`OnlineHalfLifeGate`、`MinRollingCorr`、`MinHurst`/`MaxHurst`、`SpreadRSI*`）、`EntryFilter` 与 `PairsSpec` 同名字段含义一致。

## 协整篮子：RegisterBaskets

```go
runner.RegisterBasketsParams("SABasketJohansen", func(p *runner.Params) (runner.BasketsSpec, error) {
	return runner.BasketsSpec{
		Baskets: []runner.BasketDef{
			{Name: "majors", Symbols: []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"}},
		},
		Window:               p.Int("window", 240),
		DifferenceLags:       p.Int("differenceLags", 1),
		WeightMethod:         "johansen",
		RequireCointegration: true,
		Confidence:           0.95,
		Band: statistics.BandConfig{Entry: 2, Exit: 0.5, Stop: 4, MaxHold: 96},
	}, p.Err()
})
```

每个篮子含 2 到 12 个互异品种。`WeightMethod` 取 `johansen`（首个协整向量，窗口须不小于 `腿数×(DifferenceLags+1)+3`）、`multivariate_ols`（第 0 腿对其余腿回归）或 `fixed`（显式 `Weights`）。`RequireCointegration` 要求当期 Johansen 秩检验（`Confidence` 取 0.90/0.95/0.99）通过才开新仓。持仓期默认冻结入场权重，`DynamicWeights: true` 持续刷新。`GrossPerBasket`、`MaxGross`、`DriftBand`、`WeightMode` 与配对同义。`RegisterBaskets(name, spec)` 接受固定的 spec，`RegisterBasketsParams` 允许从 `params` 读取。

## 残差 s-score：RegisterResidualStatArb

按 Avellaneda 与 Lee（2010）的做法，在可投资宇宙上提取系统因子，对每个资产的累计残差做 OU 拟合得到中心化 s-score：

```go
runner.RegisterResidualStatArbParams("SAResidualPCA", func(p *runner.Params) (runner.ResidualStatArbSpec, error) {
	return runner.ResidualStatArbSpec{
		FactorModel: "pca", NumFactors: p.Int("numFactors", 3),
		Window: p.Int("window", 60), EntryS: 1.25, StopS: 3, MaxHoldBars: 48,
		WeightMode: "inverse_sigma", MaxWeightPerAsset: 0.15,
	}, p.Err()
})
```

- `FactorModel`：`pca`（标准化收益的前 `NumFactors` 个主成分）或 `benchmarks`（`Benchmarks` 指定 1 到 16 个基准品种，如 BTC、ETH，须在 `pairs` 中）。
- `s <= -EntryS` 做多，`s >= EntryS` 做空，回到 `ExitLongS`/`ExitShortS` 以内止盈，`|s| >= StopS` 或持仓满 `MaxHoldBars` 平仓；`MinHalfLife`/`MaxHalfLife` 是 OU 半衰期入场闸门。
- 目标组合经投影严格满足美元中性（权重和为 0）与因子 beta 中性；`TargetGross`、`MaxWeightPerAsset`、`DriftBand` 控制总敞口、单资产上限与漂移缓冲。
- 常与[滚动流动性宇宙](./factor.md#时点滚动流动性宇宙)联用（YAML `universe`），只在当时最活跃的品种上估计。

## 现货加永续：carry

同交易所的现货与永续可放进同一次运行。根配置声明 `market_type: linear` 与 `extra_markets: [spot]`，`pairs` 同时列出现货 `BTC/USDT` 与永续 `BTC/USDT:USDT`，资金费使用 `funding_policy: storage`（只对永续腿结算，现货腿为 0）：

```go
runner.RegisterPairs("CarryBasis", func(p *runner.Params) (runner.PairsSpec, error) {
	return runner.PairsSpec{
		Pairs:       []runner.Pair{{Y: "BTC/USDT:USDT", X: "BTC/USDT"}},
		Window:      p.Int("window", 168),
		SpreadModel: "ratio",
		Band: statistics.BandConfig{
			Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
			Stop: p.Float("stop", 5), MaxHold: p.Int("maxHold", 72), ShortOnly: true,
		},
		LongOnlySpot: true,
		Gross:        p.Float("gross", 0.5),
	}, p.Err()
})
```

```yaml
market_type: linear
extra_markets: [spot]
pairs: ['BTC/USDT', 'BTC/USDT:USDT']
execution: {mode: weights, funding_policy: storage}
```

永续相对现货溢价时（比值 z 高于入场阈值）卖出永续、买入等额现货，价差回归时平仓。账本禁止裸做空现货；`ShortOnly` 禁止反向价差，`LongOnlySpot` 拦截任何会让现货腿变空的入场（诊断 `pair-spot-short-blocked`）。数据库需要覆盖回测窗口内的现货 K 线、永续 K 线（1h 与 1m）以及 `funding_rate` 数据。这是基差回归交易而不是持有到交割的套利，收益来自基差收敛与空永续腿的资金费，两者都可能为负。完整配置见 [carry 示例](https://github.com/banbox/banstrats/tree/main/examples/carry)。

## 结果解读

先检查 `run.json` 的完成状态，再看 `Results[]`（字段使用 Go 名称，首字母大写，同样出现在 `run-completed` 事件中）。

- `Result.Pairs` 仅由 `RegisterPairs`、`RegisterRollingPairs` 与篮子的历史回放汇总，每项含 PnL、Trades、Wins、WinRate、InMarketBars、PausedBars、AvgHoldBars、LastBeta、Turnover 与 Episodes。Episodes 有数量上限，完整次数见 `EpisodeCount`；出场原因为 `exit`、`stop`、`maxhold`、`leg_missing`、`end`（滚动重选的立即换期为 `reselect`）。
- `Unavailable`/`UnavailableLeg` 标明整次跳过的配对；`AllPairsUnavailable` 为真表示没有可运行配对，平直净值不能当作有效策略表现；`PausedPeriods` 统计所有配对同期暂停的轮次。
- 逐对 `PnL` 是按决策价与各对目标权重计算、占初始 NAV 的比例，未分摊成本，不等于成交盈亏。对账用 `PairsGrossPnL`（各对合计）、`PairsNAVReturn`（账本收益率）与 `PairsResidual = PairsNAVReturn - PairsGrossPnL`（含成本、延迟、权重漂移和共享品种净额成交的近似差异）。
- `Result.Attribution` 仅在 weights 账本提供多空腿盈亏、敞口、费用、滑点、资金费与 `TopSymbols`，用于发现单一品种贡献过大。

## 限制与注意事项

- **回测与实盘**：上述注册接口的示例面向 weights 回测。实盘仅支持线性永续、净持仓，现货腿、期权等被实盘路径拒绝；两腿异步成交会产生对冲误差，已有持仓须先归因对账。实盘接入与限制见[多因子实盘接入](./factor.md#实盘接入)。本文无法证明配对策略已完成真实交易所端到端验收。
- **样本偏差**：用全样本挑选的“协整对”再回测属于选择偏差。固定配对示例只是常见组合，滚动重选更接近可交易流程；数值上通过检验不等于显著或可盈利。
- **成本**：每次往返要付手续费、滑点，永续还有资金费；请核对 `(Entry - Exit) × 残差波动` 是否明显大于往返成本。
- **权重漂移计费**：`DriftBand` 为 0 时，Weights 账本每轮按目标权重再平衡并对漂移部分计费。
- **窗口与缺口**：窗口是最近 N 个有效观测，不是日历 bar；某腿缺失的 bar 不进入窗口。
- 示例的测试只覆盖配置解析、参数校验和合成数据下的行为，没有用真实行情验证。

相关页面：[多因子与截面策略](./factor.md)、[回测](./backtest.md)、[因子 API](../api/factor.md)。
