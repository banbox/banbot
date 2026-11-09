# banbot 统计套利：配对策略、数值 API 与研究运行合同

前提已掌握时序策略文档（DataSeries/Kline、公共类型、通用配置）与截面因子文档（runner、YAML 通用字段、Frame、portfolio、Full/Patch、`Params`、`RegisterDefinition`/`RegisterPortfolioFactory`、实盘通用约束、价格流与过期默认值）；本文只写统计套利专属内容。

## 0. 总览与术语

唯一链路：**可见数据 → 训练（离线、冻结）→ 当期信号 → 目标组合 → 成交与账户证据**。

| 环节 | 产物 |
| --- | --- |
| 训练（调用方 Go 代码，只读训练区间） | β/α、残差尺度、阈值、持有期、候选参数 |
| 冻结（写 YAML `params`，进 manifest hash；或 ParameterArtifact/ModelArtifact） | 不可变参数 |
| 信号（runner 每决策轮在线算） | 每对 z-score、带状态仓位 ±1/0 |
| 目标（`RegisterPairs` builder 输出不可变 `factor.TargetPortfolio`：`ID()` 内容哈希，`Targets()` 返回 SID→NAV 比例副本） | 目标权重、诊断码 |
| 证据（Weights/Events 账本及其输出文件） | 成交、费用、资金费 |

术语：`Y` 被解释腿（做多价差时买入）；`X` 解释腿（做多价差时卖出）；`z` = 价差残差/残差标准差；`β` 对数回归斜率（弹性）；决策 bar = `run_timeframes` 对应已收盘 K 线。统计函数不读时钟、账户、交易所；数据经 `DataSeries.Values` 传入，保留类型与 NULL，缺失须显式排除，不得当 0。

## 1. 防未来函数与可见性规则

1. 选对、选方向、拟合 β/α/残差尺度/阈值/持有期/参数候选：只用 `EndAt <= TrainEnd` 且 `AvailableAt <= 冻结时刻` 的观测。`statistics` 函数不做时间过滤，由调用方裁剪。
2. **配对池选择属于训练**：全样本挑出的“协整对”回测是选择偏差。
3. `research.SelectParameters` 只保留 `BeginAt >= TrainStart`、`EndAt <= TrainEnd`、`AvailableAt <= spec.AvailableAt` 的观测，仅对保留行校验 `SID>0`、`Candidate` 非空、`EndAt > BeginAt`、`AvailableAt >= EndAt`、`NetReturn` 有限。`research.ResolveParameters(artifacts, asof, manifestID)` 只接受 `ManifestID` 匹配、`AvailableAt <= asof`、`TrainEnd < asof`、ID 为内容哈希的产物，取 AvailableAt 最新者（并列取 ID 更大者）；无可见产物或损坏均报错。
4. 在线 `RollingSpread` 只用当前及过去已收盘 bar；窗口拟合包含当前 bar（决策时点已收盘），不含未来 bar。
5. 执行只用决策后、`LatencyMS` 之后且严格晚于决策时间的价格；Events 需 tick 或 1m 流（K 线 OHLC 不能重建盘中路径）；`DecisionDelayMS` 推迟可见截止。
6. `KalmanPair.Observe` 的 `Error`/`Variance` 是**更新前**预测新息，可直接作当期标准化残差；`FitKalmanNoise` 的 Q/R 只能由训练样本拟合并冻结。
7. 静态 SQL 最新值表只能声明 `data.pit_policy: static-approximation`；严格 PIT 需不可变 archive 或带证据的版本页 provider。
8. 测试窗口结果不得用于选参；`asof` 只保证可见性，不防人工回看后改参，此类改动计入试验次数。
9. 协整检验统计量须与匹配确定性项的临界值比较（3.1），不能用普通 ADF/正态表。
10. 数值合法（有限、可识别、收敛）≠ 显著或可盈利。

## 2. 配对策略：`runner.RegisterPairs`

### 2.1 签名与类型

```go
func RegisterPairs(name string, build func(*Params) (PairsSpec, error)) error
type Pair struct{ Y, X string }          // 与运行 SIDMap 中符号完全一致，如 "ETH/USDT:USDT"
type PairsSpec struct {
    Pairs  []Pair
    Field  string                        // kline 字段，缺省 "close"
    Window int                           // 回归窗口（决策 bar 数），>= 3
    Band   statistics.BandConfig         // 见 2.3
    Gross  float64                       // 单对总名义占策略 NAV；0 = 1/len(Pairs)
}
```

`Params`（`Float/Int/Bool/Err/Unused`）见截面因子文档；相关行为：`Int` 小数报错不截断；`Bool` 仅 0/1；`Unused()` 忽略 `window`、`k`，**不会自动执行**，须在 build 中自行检查并返回错误，否则拼错的参数名静默回退默认值。**无字符串读取方法**，`Pairs`、`Field` 不能由 YAML 参数化，不同品种组合须注册不同名字。

`build` 同一次运行被多次调用（定义编译、预检、工厂创建），须为纯函数，无 IO/全局副作用。

注册占用定义名 `name` 与工厂名 `name-pairs-v1`（均不能已注册；先注册定义名，工厂失败时定义名已被占用）。构造器自动绑定，YAML 只需 `name`；显式 `portfolio.builder` 优先。

### 2.2 校验（build 返回后）

`build`/`name` 为空→注册失败；`Params.Err()` 非空→返回该错误；`Field==""`→`"close"`；`len(Pairs)==0`、`Window<3`、`Band.Validate()` 失败→错误；`Gross==0`→`1/len(Pairs)`；`Gross<=0` 或 `Gross*len(Pairs) > 1+1e-9`→错误（总敞口不得超 NAV）；`Y==""`/`X==""`/`Y==X`→错误；Y/X 不在运行 `SIDMap`→构造组合构造器时报错（`PreparePortfolioConfig`，资源装配前）。

构造期不检查可交易性（见 2.4）。

### 2.3 带状态信号机：`statistics.BandConfig` / `BandTrader`

```go
type BandConfig struct {
    Entry, Exit, Stop float64
    MaxHold           int   // 决策轮数，0 关闭
    LongOnly          bool  // 禁止做空价差 (z >= Entry)
    ShortOnly         bool  // 禁止做多价差 (z <= -Entry)
}
func (c BandConfig) Validate() error
func NewBandTrader(cfg BandConfig) (*BandTrader, error)   // 每个价差一个实例
func (b *BandTrader) Step(z float64) int                  // -1/0/+1
func (b *BandTrader) Position() int
```

校验：阈值有限；`Entry>0`；`0<=Exit<Entry`；`Stop==0 或 Stop>Entry`；`MaxHold>=0`；LongOnly 与 ShortOnly 互斥。**无内置阈值默认值**（`p.Float("entry", 2)` 的 2 是调用方选择）。

状态转移：
1. 每次 `Step` 先：`blocked` 且 `|z|<=Exit` → 清除。
2. 非有限 z：返回当前仓位，状态不变。
3. 持仓：`held++`，按优先级：`Stop>0 且 |z|>=Stop` → 平仓并置 `blocked`；`|z|<=Exit` → 平仓（不置 blocked）；`MaxHold>0 且 held>=MaxHold` → 平仓并置 `blocked`。
4. 空仓：`blocked` 或 `|z|>=Stop`（Stop>0）→ 保持 0；否则 `z<=-Entry` → +1（做多价差），`z>=Entry` → -1（做空价差），受 LongOnly/ShortOnly 限制；入场 `held=0`；持仓不会单步翻向。

`blocked`：止损/到期平仓后，z 回到 `[-Exit,Exit]` 才允许再入场。

### 2.4 运行时语义（每个决策轮）

每对持有私有 `RollingSpread`、`BandTrader`、冻结 β、仓位。每轮：

| 步骤 | 条件 | 行为 | 诊断码 |
| --- | --- | --- | --- |
| a | Y/X 的 `pairs-log-v1` 值缺失或非 Valid（含价格<=0） | 跳过该对；不推进窗口与状态机；已有仓位保持 | `pair-input-missing` |
| b | `RollingSpread.Observe` 未就绪（窗口未满、x 方差<=1e-12、残差 std<=1e-12、非有限） | 跳过；不推进 BandTrader | `pair-warmup` |
| c | 就绪，某腿不可交易，当前仓位 0 | 不开仓，不推进 BandTrader | 无 |
| d | 就绪，某腿不可交易，当前有仓位 | 仍推进 BandTrader（可按规则平仓） | 无 |
| e | `Step(z)` 后仓位≠0 且原仓位为 0 或方向变化 | 冻结 β = 本期 `SpreadPoint.Beta` | 无 |
| f | 所有对都未就绪（均在 a 或 b） | 返回 nil 目标 + `pairs-not-ready` | `pairs-not-ready` |
| g | 其他 | 对每个仓位≠0 的对（含本轮输入缺失者，用其状态）计算权重输出；非空诊断附到目标 | 本轮诊断 |

权重（`pos∈{-1,0,+1}`，`G`=该对 Gross，`β` 冻结值，`N=1+|β|`）：`w_Y += pos·G/N`，`w_X += -pos·G·β/N`。同一 SID 出现在多对时**叠加**，净额可对消，零权重删除。单对 |w_Y|+|w_X| 恰为 G，总敞口 ≤ ΣG ≤ NAV。`pos=+1` 买 Y 卖 X；X 名义 = β×Y 名义（对数模型弹性对冲）。

- **β 为负时两腿同向**：代码不拒绝，须在训练/参数层过滤。
- **信号/对冲不一致**：z 用每轮滚动 α/β，持仓权重用**入场时冻结**β；持仓期间 β 漂移改变 z 而不改对冲比例。
- **窗口**=最近 N 个**可用观测**，非日历 bar；某腿无效的 bar 不进窗口，覆盖时间会拉长。
- **Full/Patch**：目标为 NAV 比例；Full 下未入目标的 SID 视为 0。平仓对被从目标删除，故**不得设 `portfolio.mode: patch`**（Patch 只改目标内 SID，未列出的仓位永不平仓）；缺省 full。
- **nil 目标 vs 空目标**：nil（f）= 本轮无新目标；引擎不替换待执行目标，Weights 账本数量不变（沿用此前持仓），`Result.Skipped++`，评估权重不更新；已提交未成交目标仍按窗口执行或过期。非 nil 空 map（所有对平仓）= Full 下全部清零，无需报价即可执行。预热期（窗口未满）为 nil，就绪后无仓位即为空目标。
- 单腿整根 kline 缺失时该轮无决策回调（`pair-input-missing` 对应值无效/价格<=0）。

### 2.5 YAML

只写 `name` 与参数；根配置（`data.pit_policy`、`execution.funding_policy`、`execution.mode`、`prices` 等）按通用文档。

```yaml
pairs: ['ETH/USDT:USDT', 'BTC/USDT:USDT']
run_policy:
  - name: eth-btc
    id: pair1
    engine: factor
    run_timeframes: [1h]
    params: {window: 120, entry: 2, exit: 0.5, stop: 4, maxHold: 0, gross: 0}
```

- `params` 键即 build 读取键；未知键仅在 build 调用 `Unused()` 时报错；改任一参数即新策略身份（manifest hash）。
- 加载器把 `params.window` 写入 `Factor.Window`、`params.k` 写入 `portfolio.K`（配对不用）；配对窗口由 `PairsSpec.Window` 决定，宜与 `window` 一致。`swapPerBars`/`holdBars` 在因子策略中被加载器拒绝（`maxHold` 可用）。
- `research` 命令对配对只输出 `logp` 面板与标签诊断；绩效来自 `backtest --mode weights|events`。

### 2.6 价格流、成交时点与成本（Weights）

**默认价格流**：Weights 且未写 `prices` 且无表达式时 = 决策周期 K 线 close（Events 存储回放默认 `kline 1m close`；归档有 tick 流时默认 `{tick,event,price}`）。

**可观察时点**：目标须在报价 `AtMS > DecisionTime`、`AtMS >= DecisionTime+latency_ms`、`AvailableAt <= now` 且在 `expiry_ms`（默认 60000）内的价格执行。故 **1h 决策 + 默认 1h close 价格流 + 默认 expiry**：下一根报价在 1h 后，非空目标全部过期，零成交（实测：账本无持仓、费用 0）。解决：配 `prices: {source: tick, timeframe: event, field: price}` 或 `kline/1m/close`（实测 tick 流在决策后 1ms 成交）。仅提高 `expiry_ms` 让其在下一根 K 线成交未实测，且成交价为下一根收盘。

**Weights 账本对漂移计费**：账本每次执行目标都按 `qty = w × spec.Budget.NAV / price` 重算数量（NAV=决策时账本 NAV，随盈亏复利），与现有数量之差按 `fee_rate`+`slippage_rate` 计费。配对每个决策轮都输出目标，所以**持仓期间价格漂移每轮被再平衡回目标权重并计费**（实测权重恒定时费用仍逐轮累计）。Gross 是当前 NAV 比例而非初始 NAV。`research.Evaluate` 的 `Costs = CostRate × Σ|w−w_prev|` 只计权重差、不含漂移，与账本口径不同（`Result.Book` 为账本口径）。Events 路径是否同样产生漂移再平衡成交未实测。

### 2.7 自定义路径（静态 β、Kalman、多腿篮子）

`RegisterPairs` 的 β 来自滚动 OLS，**无法注入静态 β 或 Kalman 状态**，需用通用接口：
1. `runner.RegisterDefinition(name, func(c runner.Config) (*factor.Plan, research.ComboSpec, error))`：`factor.New().Add(name, node).Compile()` 编译对数价格节点（`factor.Field("kline", field, c.Factor.TimeFrame)` + `factor.Custom(version, inputs, fn)`，同 `pairs-log-v1` 形式），组合为 `research.ComboSpec{Method: research.Equal, Columns: [...]}`。
2. `runner.RegisterPortfolioFactory(name, func(c runner.Config) (runner.PortfolioBuilder, error), validator)`：名字须含 `-v`；validator 只做纯参数检查，不构造状态。
3. 构造器：`func(frame factor.Frame, universe factor.Universe, spec factor.PortfolioSpec, def research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)`；输出 `factor.NewTargetPortfolio(spec, weights)`，`spec` 用入参。
4. YAML：`portfolio: {builder: <name>-v1}`（显式值优先）。

有状态构造器每决策轮只能调用一次（重复会重复推进滤波状态），状态须每次运行私有。

## 3. 数值 API（`factor/statistics`）

约定：观测按时间升序；矩阵行=观测、列=变量，列序拟合与预测一致。非有限输入、不可识别、维度不符、超预算均返回 error。

### 3.1 回归、价差模型与平稳性

```go
func MeanStd(values []float64, ddof int) (mean, std float64, err error)   // len<=ddof 报错；常数样本 std=0
type Regression struct { Intercept float64; Beta []float64; Residuals []float64; Rank int }
func Regress(y []float64, exposures [][]float64, intercept bool) (Regression, error)  // 秩亏/样本不足报错

type OU struct {
    Intercept, Slope, Equilibrium, InnovationStd float64
    LinearHalfLife, DiscreteHalfLife             float64  // 线性 -ln2/b；离散 -ln2/ln(1+b)
}
func FitOU(values []float64) (OU, error)  // Δx = a + b·x_{t-1}；len>=4；要求 -1<b<0 否则报错；Equilibrium=-a/b；InnovationStd ddof=2

type ADFSpec struct { Lags int; Intercept, Trend bool }
type ADFResult struct { DeltaCoefficient, StandardError, TStatistic float64; Samples, DegreesOfFreedom int }
func AugmentedDickeyFuller(values []float64, spec ADFSpec) (ADFResult, error)   // len >= Lags+4
func EngleGranger(y []float64, x [][]float64, intercept bool, lags int) (Regression, ADFResult, error)

type JohansenSpec struct { DifferenceLags int; Intercept, Trend bool }  // Trend 需 Intercept；滞后 0..128
type JohansenResult struct {
    Eigenvalues, Trace, MaximumEigenvalue []float64   // 特征值降序；Trace[r] 检验 H0: 秩<=r
    CointegratingVectors, AdjustmentVectors [][]float64 // [分量][资产]；beta'S_kk beta=I；首个最大载荷为正
    Samples, NuisanceRank int
}
func Johansen(levels [][]float64, spec JohansenSpec) (JohansenResult, error)   // 2<=资产<=64
func (j JohansenResult) TraceRank(criticalValues []float64) (int, error)       // 长度须等于 len(Trace)(=资产数)，值>0；顺序拒绝，首个不拒绝处停止
```

- **临界值（以代码为准）**：`statistics` **没有任何临界值表、插值函数或 p 值**；`AugmentedDickeyFuller`/`EngleGranger` 只返回 `TStatistic`，`TraceRank` 由调用方提供临界值，无默认表。
- **EngleGranger**：`Regress(y,x,intercept)` 后对残差做 `ADFSpec{Lags: lags}`（**残差检验无截距、无趋势**）。统计量越负越倾向拒绝“无协整”，须与匹配确定性项/回归元个数的 Engle–Granger/MacKinnon 临界值比较。外部参考（**非代码提供**）：MacKinnon 渐近值，N=2 含常数项约 1%:-3.90、5%:-3.34、10%:-3.04；加趋势、更多回归元、有限样本均不同，应取响应面或自行模拟。
- **FitOU**：按 bar 计数用 `DiscreteHalfLife`。
- **Johansen** 特征向量须先按某一腿归一再做腿权重；`TraceRank` 临界值须对应相同滞后、确定性项、显著性水平。

### 3.2 在线滚动价差（`RegisterPairs` 所用）

```go
type SpreadPoint struct {
    Alpha, Beta float64   // 当前窗口 OLS: y = alpha + beta*x
    Residual    float64   // 当前观测 y - alpha - beta*x
    Std         float64   // 窗口残差标准差，ddof=2
    Z           float64   // Residual / Std
}
func NewRollingSpread(window int) (*RollingSpread, error)  // window >= 3
func (r *RollingSpread) Window() int
func (r *RollingSpread) Observe(x, y float64) (SpreadPoint, bool)   // x 为 X 腿、y 为 Y 腿（对数价格）
```

`ok=false`：窗口未满、输入非有限（样本被跳过）、x 窗口方差<=1e-12、残差 std<=1e-12。每次为当前窗口完整 OLS（非冻结）；对数价格下 β 为弹性。

### 3.3 Kalman 滤波（随机游走 β 与截距）

```go
type KalmanPair struct {
    Beta                                 [2]float64     // [斜率, 截距]
    Covariance                           [2][2]float64  // 须 PSD
    ProcessVariance, ObservationVariance float64        // ProcessVariance>=0 同时作用于斜率与截距的对角 Q；ObservationVariance>0
    ProcessCovariance                    [2][2]float64  // 非零时使用，与非零 ProcessVariance 互斥
}
type KalmanObservation struct {
    Prediction, Error, Variance float64  // 更新前预测、新息、新息方差 S
    PriorBeta, PosteriorBeta    [2]float64
}
func (k *KalmanPair) Observe(x, y float64) (KalmanObservation, error)       // 修改状态；按时间顺序调用
func KalmanLikelihood(x, y []float64, initial KalmanPair) (float64, error)  // 负对数似然
type KalmanFitSpec struct {
    Initial                       KalmanPair
    MaxEvaluations, MaxIterations int             // 缺省 4000 / 2000
    Runtime                       time.Duration   // 缺省 30s
    DiagonalProcessNoise          bool
}
type KalmanNoiseFit struct {
    Initial                                              KalmanPair  // 拟合后的 Q（写入 ProcessCovariance，ProcessVariance=0）与 R
    InitialNegativeLogLikelihood, NegativeLogLikelihood  float64
    Evaluations                                          int
    Status                                               string
    Converged                                            bool
}
func FitKalmanNoise(ctx context.Context, x, y []float64, spec KalmanFitSpec) (KalmanNoiseFit, error)
```

- `Observe`：随机游走状态，观测 `y = β·x + 截距`；标准化残差 `Error/sqrt(Variance)` 可替代滚动 z 交给 `BandTrader`。
- `FitKalmanNoise`：`len>=8`，Nelder–Mead 最大化似然，初始状态固定；初始 Q 须正定（`DiagonalProcessNoise` 时须对角）、`ObservationVariance>0`。预算耗尽或取消返回错误；`Converged` 要求无提前终止且最终负对数似然不高于初始（容差 1e-8）。

### 3.4 局部水平、EMA 与 AR

```go
type LocalLevel struct{ Mean, Variance, ProcessVariance, ObservationVariance float64 }  // ObservationVariance>0
type LevelObservation struct{ Prediction, PredictionVariance, Innovation, Gain float64 }
func (k *LocalLevel) Observe(value float64) (LevelObservation, error)
type LocalLevelFitSpec struct { Initial LocalLevel; MaxEvaluations, MaxIterations int; Runtime time.Duration }  // 缺省 3000 / 2000 / 15s
type LocalLevelFit struct { Initial LocalLevel; NegativeLogLikelihood, InitialNegativeLogLikelihood float64; Evaluations int }
func FitLocalLevel(y []float64, s LocalLevelFitSpec) (LocalLevelFit, error)   // >=8 观测，初始 Q>0
func LocalLevelEffectiveEMA(q, r float64) (gain, period float64, err error)   // 稳态增益 2/(1+sqrt(1+4r/q))，period=2/gain-1
func LocalLevelForEMA(mean, period, r float64) (LocalLevel, error)            // period>1；令 Q/R=4/(N²-1)，每步用精确 EMA 增益
type ARModel struct{ Intercept float64; Coefficients []float64 }             // lag1 在前
func FitAR(values []float64, lags int) (ARModel, error)                     // 条件 OLS（非 ARMA MLE）；len>=2*lags+2
func (m ARModel) Forecast(history []float64) (float64, error)
```

稳态 EMA 增益只是渐近结论。

### 3.5 Vidyamurthy 滤波、滞后选择与成交核对

```go
type SpreadObservation struct{ Prediction, PredictionVariance, Filtered, Variance, SlopeWeight float64 }
func NewLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error)       // lag 1..64；processVariance>=0；initialVariance>0
func NewSourceLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error) // lag>=3 用源书不相关近似
func (k *LaggedSpreadFilter) Observe(value, observationVariance float64) (SpreadObservation, error)                           // observationVariance>0
func SelectSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (lag int, score float64, filter *LaggedSpreadFilter, err error)
func SelectSourceSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (int, float64, *LaggedSpreadFilter, error)  // 用 Source 构造器，其余同上

type PairedFill struct{ Price, Quantity float64 }                  // 均须 >0
type PairedExecution struct {
    Pairs []FillPair                                               // FillPair{BidderIndex, TargetIndex int; TargetQuantity float64}
    MatchedTarget, BidderEquivalent, TargetTotal, AverageSpread float64
    AverageSatisfies, FullyPaired bool
}
func PairExecutions(bidder, target []PairedFill, ratio, cash, limit float64, putOn bool) (PairedExecution, error)
func CompareCommonFactors(a, b []float64, covariance [][]float64) (CommonFactorPair, error)
func InterpolateMonotoneTail(counts []float64) ([]float64, error)   // 非增非负；变化点间线性插值去平台
```

- `SelectSpreadLag`：`len(values)>=2`；以 `values[0]` 为初值，从第 2 个观测起累计 SSE（测量误差²+预测误差²）；候选顺序决定并列；返回的滤波器可继续在线使用。
- `PairExecutions` 是**成交后核验器**（不下单、不保证同时成交）：`ratio>0`；bidder 价格×ratio、数量÷ratio 换算成 target 单位；put-on 要求 `ratio*bidder+cash-target >= limit`，unwind 要求 `<= limit`；`AverageSpread` 为加权平均价差；按“卖价升序、买价降序”饱和配对。两侧各 ≤10000 笔、乘积 ≤1e6。
- `CompareCommonFactors`（Vidyamurthy ch6）：`a`、`b` 为两资产对 n 个公共因子的暴露向量，`covariance` 为 **n×n 因子协方差矩阵**（n=len(a)=len(b)，须对称半正定；**不是资产协方差，不含特有方差**）。返回 `CommonFactorPair{Correlation, AbsoluteCorrelation, GammaAB, GammaBA, ResidualVarianceAB, ResidualVarianceBA}`：aa=a'Σa、ab=a'Σb、bb=b'Σb；`Correlation=ab/√(aa·bb)`；`GammaAB=ab/bb`（B 对冲 A）、`GammaBA=ab/aa`；`ResidualVarianceAB=aa−ab²/bb`、`ResidualVarianceBA=bb−ab²/aa`（下限 0）；aa 或 bb<=0 报错。

### 3.6 阈值、持有期与正则化

```go
func GaussianThreshold(sigma float64) (float64, error)  // 最大化 δ(1-Φ(δ/σ))，≈0.75179σ；sigma>0
func SmoothThresholdCounts(counts []float64, lambda float64) ([]float64, error)  // min ||c-z||² + λΣ(Δz)²
type ThresholdFit struct { Threshold, Objective float64; Counts, Smoothed []float64 }
func EmpiricalThreshold(spreads, grid []float64, lambda float64) (ThresholdFit, error)   // spreads 须已居中
type RegularizationChoice struct { Lambda float64; FitErrors, Smoothed []float64 }
func RegularizationHeel(counts, lambdas []float64) (RegularizationChoice, error)         // len(counts)>=2，len(lambdas)>=3 且严格递增为正
func CrossingDurations(spreads []float64) ([]int, error)            // 零点完整穿越间隔（观测数）
func DurationQuantile(durations []int, q float64) (float64, error)  // q∈[0,1]，线性插值，间隔须>0
type DurationPopulation struct { Lower, Median, Upper float64; Samples int }  // Samples=draws
func BootstrapDurations(intervals []int, draws int, seed uint64, lower, upper float64) (DurationPopulation, error)
```

- `GaussianThreshold` 不含成本与资金费；σ=1 约 0.7518，远低于常用 2；实际 Entry 须按成本重算（7.1）。
- `EmpiricalThreshold`：`Counts[i]` 为两侧超阈值计数平均，经 `SmoothThresholdCounts` 平滑后选 `threshold·z` 最大者（无正值时 `Threshold=grid[0]`）；`grid` 严格递增且非负。`RegularizationHeel` 在 λ 内部点选归一化曲率最大者，**无正曲率时返回 `lambdas[0]`**，调用方应检查。
- `CrossingDurations`：出现相反符号非零值才确认穿越（零值跳过），首尾删失区间不计。
- `BootstrapDurations`：`len(intervals)>=2`、值>0，重抽样估计总体分位数；`lower∈[0,0.5)`、`upper∈(0.5,1]`、`draws∈[2,1e6]`；假设 i.i.d.，序列相关时区间偏窄。

### 3.7 组合、因子与轮动

```go
type NeutralAllocation struct { Weights []float64; ConstraintRank int; Gross float64; Scaled, NoResidualAlpha bool }
func FactorNeutralWeights(alpha, variance []float64, exposures [][]float64, risk, grossLimit float64) (NeutralAllocation, error)
```
最大化 `α'w − (risk/2)·w'Dw`，约束 `exposures'w=0`；`variance`=独特方差（>0）；`risk>0`；全 1 列即美元中性；`grossLimit>0` 时按比例缩放（保持中性，非精确 L1）。

```go
type PCAModel struct { Mean, Scale, Eigenvalues []float64; Loadings [][]float64 }  // Loadings[分量][特征]，特征值降序
func FitPCA(rows [][]float64, components int, standardize bool) (PCAModel, error)   // rows>=2，1<=components<=特征数
func (m PCAModel) Transform(values []float64) ([]float64, error)
func (m PCAModel) Residual(values []float64) ([]float64, error)
type FactorAnalysisConfig struct { Components, MaxIterations int; Tolerance, MinUniqueness float64; ZeroMean bool }  // MaxIterations∈[1,1e4]、Tolerance>0、MinUniqueness>0 均无缺省，必须给
type FactorAnalysisModel struct { Mean []float64; Loadings [][]float64; Uniqueness []float64; NegativeLogLikelihood float64; Iterations int; FloorActive bool }  // Loadings 行=资产
func FitFactorAnalysis(ctx context.Context, rows [][]float64, config FactorAnalysisConfig) (FactorAnalysisModel, error)  // 1<=Components<资产数<=256，rows>=Components+2
func (m FactorAnalysisModel) FactorScores(row []float64, posterior bool) (FactorScoreResult, error)   // Scores, Residuals
func VarimaxLoadings(rows [][]float64, kaiser bool, maxIterations int, tolerance float64) ([][]float64, error)   // rows 为载荷矩阵(资产×因子)，资产<=256，因子<=资产；旋转保持 LL'
func WinnerMinusLoserReturn(previous, current, lookback []float64) (float64, error)
func HierarchicalRiskParity(cov [][]float64) (HRPResult, error)   // HRPResult{Weights []float64; Order []int}；边际方差>0，对称 PSD
func LongOnlyMinimumVariance(cov [][]float64, maxIterations int, tolerance float64) (MinimumVarianceResult, error)
func ChanLongOnlyFrontier(mean []float64, cov [][]float64) (ChanFrontierResult, error)   // 2..12 资产；21 个目标点；cov 须正定；结果含 Tangency、MinimumVariance 索引
func ChanInverseRiskWeights(risk []float64) ([]float64, error)
func ChanSignedTangency(mean []float64, cov [][]float64) ([]float64, error)
```
以上只输出数值，**不生成交易日程**，选择规则与调仓节奏由调用方给出。多资产组合可用 HRP/最小方差作权重基线，对冲腿 β 结构仍来自第 4 节流程。

### 3.8 不属于主线的模块

AFML 标签/CPCV、订单流与 PIN/VPIN、VID 风险定价、HMM（`FitBinaryHMM`）、GARCH/ARMA/ARIMA/VAR（`FitVAR`、`SelectVAR`、`FitARMA`、`FitARIMA`、`FitGARCH`）、SVM/树/神经网络等；通常不需要，使用前核对函数注释。

## 4. 训练 → 冻结 → 信号 → 目标 → 成交证据

### 4.1 训练流程（离线，只用训练区间）

1. 训练对数价格：决策周期 `log(close)`，按 `EndAt <= TrainEnd` 与 `AvailableAt` 裁剪，两腿对齐，缺失整行删除，不前向填充。
2. 每个候选对以 Y/X 两种方向做 `EngleGranger`（截距显式），比较残差 ADF 统计量与 β 稳定性，由训练结果定方向。
3. `FitOU(残差)` 取 `DiscreteHalfLife`。Window 经验起点约 3–5 倍离散半衰期且不少于 30 bar——**纯经验法则，无源书/代码/测试依据**，须做敏感性检验。
4. 残差标准化为 z（训练均值/标准差），`EmpiricalThreshold`+`RegularizationHeel` 选 Entry；Exit/Stop 由调用方选（如 Entry 的 1/2 与 2 倍），满足 `Stop > Entry > Exit >= 0`。
5. `CrossingDurations`+`BootstrapDurations` 的分位数用于估 `MaxHold` 量级，非确定截止时间。
6. 成本门槛：按 7.1 确认 `(|Entry|-|Exit|)·σ/(1+|β|)` 覆盖往返成本，否则提高 Entry 或放弃。
7. 多腿篮子：`Johansen`（需对应临界值）；`RegisterPairs` 仅两腿，多腿走 2.7。

### 4.2 冻结产物

- **配对策略**：结果直接写 YAML `params`（`window/entry/exit/stop/maxHold/gross`）；训练记录（区间、数据版本、统计量、λ、候选）由调用方保存。
- **参数候选统计选择**：`research.SelectParameters(observations []ParameterObservation, spec ParameterSelectionSpec) (ParameterArtifact, error)`。
  - `ParameterObservation{SID, Group, Candidate, BeginAt, EndAt, AvailableAt, NetReturn}`（净收益，已扣成本）。
  - `ParameterSelectionSpec{TrainStart, TrainEnd, AvailableAt, MinIndependentSamples(>=2), PriorSamples(>=0), ConfidencePenalty(>=0), HACLag(>=0), ManifestID, AlgorithmVersion(均非空), Candidates map[string]map[string]float64}`；要求 `TrainStart>0`、`TrainEnd>=TrainStart`、`AvailableAt>=TrainEnd`。
  - `Score = Mean − ConfidencePenalty·SE`（HAC 标准误）；资产层按 `n/(n+PriorSamples)` 向组、全局收缩；非重叠 episode 计为独立样本。
  - **库内无运行时消费者**：`SelectParameters`/`ResolveParameters` 结果不会自动注入 `RegisterPairs` 参数，须调用方映射回 YAML/Go 代码。
- **非线性/回归模型**：`research.FitModel(name, manifest string, features []string, params json.RawMessage, rows []TrainingSample, window TrainingWindow, availableAt int64) (ModelArtifact, Predictor, error)`（`availableAt >= window.FitAsOf`），`PublishModel(dir, artifact)` 写不可变内容寻址文件，`LoadModel(path, asof)` 恢复；须先 `RegisterModel(name, factory)`。`TrainingWindow{Start, End, FitAsOf, TestStart, TestEnd, EmbargoMS, MinSamples}`；`VisibleTrainingRows` 仅留 `DecisionTime∈[Start,End]`、`MatureAt/AvailableAt <= FitAsOf`、`LabelEnd < TestStart-EmbargoMS` 的样本并校验时序。

### 4.3 验收顺序

1. `go test ./factor/statistics ./factor/research ./factor/runner ./execution` 与 `go vet`：只证明软件合同。
2. `banbot backtest --mode weights --config runtime.yml`：确认信号、目标、诊断码、持仓天数、β 符号、两腿名义比例；**须确认价格流使非空目标真正成交**（2.6）。
3. `banbot backtest --mode events --config runtime.yml`：加显式价格流（tick 或 1m）、成交模型与合约单位。
4. `banbot trade --dry-run --config runtime.yml`：检查可见性、诊断与订单前检查，无真实成交。
5. 实盘（第 6 节）须有场所级证据。

## 5. 执行：跨场所机会与模拟账本

### 5.1 机会估计（纯函数，`statistics`）

```go
type CrossVenueQuote struct {
    Venue, Asset, Currency                string
    Bid, Ask, BidSize, AskSize, FeeRate   float64
    AvailableCash, AvailableInventory     float64
    ObservedAtMS                          int64
}
type CrossVenueOpportunity struct {
    BuyVenue, SellVenue                               string
    Units, GrossProfit, Fees, TransferCost, NetProfit float64
}
func CrossVenueArbitrage(quotes []CrossVenueQuote, nowMS, maxAgeMS int64,
    units, transferRate, fixedTransferCost, minProfit float64) (CrossVenueOpportunity, bool, error)
```

- 2–1000 个报价；`nowMS>0`、`maxAgeMS>=0`、`units>0`；`transferRate∈[0,1)`、`fixedTransferCost>=0`、`minProfit>=0`；每个报价 `Venue/Asset/Currency` 非空、`0<ObservedAtMS<=nowMS`、`Bid>0`、`Ask>=Bid`、`FeeRate∈[0,1)`、尺寸/资金/库存 >=0，否则报错。
- 仅配对**不同场所、同 asset/currency、未过期**（`now-ObservedAtMS<=maxAgeMS`）的报价（不同 asset 跳过不报错）。
- 买入侧需 `AskSize>=units`、`AvailableCash >= units·Ask·(1+fee)`；卖出侧需 `BidSize>=units`、`AvailableInventory>=units`。
- `gross=units·(sellBid-buyAsk)`；`fees=units·(sellBid·feeS+buyAsk·feeB)`；`transfer=units·buyAsk·transferRate+fixedTransferCost`；返回净利润最高且 **严格大于** `minProfit` 的方向。
- 仅机会计算器：不保证原子结算、不含场所信用风险与 FX。

### 5.2 模拟账本（`execution.CrossVenuePaper`，独立于单账户协调器）

```go
type CrossVenueBalance struct{ Venue, Asset, Currency string; Cash, Units decimal.Decimal }
type CrossVenueLevel struct{ Price, Units decimal.Decimal }
type CrossVenueBook struct {
    Venue, Asset, Currency        string
    AtMS, PublishedMS, ReceivedMS int64               // AtMS <= PublishedMS <= ReceivedMS <= now；now-AtMS <= MaxAgeMS
    Bids, Asks                    []CrossVenueLevel    // 各 1..1024 档、价格严格有序（买降卖升）、最优买价<=最优卖价
    FeeRate                       decimal.Decimal      // [0,1)
}
type CrossVenueRequest struct {
    ID                                        string   // 规范 ID
    Buy, Sell                                 CrossVenueBook
    Units, MinProfit, EstimatedRebalanceCash  decimal.Decimal   // Units>0；其余>=0
    NowMS, MaxAgeMS                           int64
}
type CrossVenueEstimate struct{ BuyNotional, SellNotional, BuyFee, SellFee, NetProfit decimal.Decimal }
type CrossVenueTransfer struct {
    ID, From, To                  string
    Units, Cash, UnitFee, CashFee decimal.Decimal
    AtMS, ArrivalMS               int64
    CompletedMS                   int64; Completed bool
}
func OpenCrossVenuePaper(absPath string, initial []CrossVenueBalance) (*CrossVenuePaper, error) // 绝对路径；2–1000 场所；SQLite；重开须创世余额一致
func (p *CrossVenuePaper) Close() error
func (p *CrossVenuePaper) Snapshot(ctx context.Context) (CrossVenueSnapshot, error)
func EstimateCrossVenue(r CrossVenueRequest) (CrossVenueEstimate, error)       // 只读，沿深度计算；NetProfit 已减 EstimatedRebalanceCash；深度不足返回 ErrCrossVenueDepth
func (p *CrossVenuePaper) Begin(ctx context.Context, r CrossVenueRequest) (CrossVenueTrade, error) // 只提交买腿，状态 "bought"
func (p *CrossVenuePaper) Exit(ctx context.Context, id string, book CrossVenueBook, now int64, unwind bool) (CrossVenueTrade, error)
func (p *CrossVenuePaper) StartTransfer(ctx context.Context, r CrossVenueTransfer) error
func (p *CrossVenuePaper) CompleteTransfer(ctx context.Context, id string, now int64) error
func (s CrossVenueSnapshot) Equity(mark decimal.Decimal) (decimal.Decimal, error)
```

- `Begin` 检查买入场所预置现金、卖出场所现货库存与扣除再平衡补贴后的净边际。
- `Exit(unwind=false)` 用调用方提供的可见 book 在卖出场所卖出，`complete`；`unwind=true` 用新鲜 bid book 在买入场所反向卖出库存，`unwound`。`book` 须属对应场所；相同参数重复调用幂等，冲突参数报错。
- 两腿非原子：买腿成交后卖腿失败不回滚，停在 `bought` 暴露状态，阻止新交易与转出直到完成或解除（转入完成仍可）；卖出可能亏损。
- 转账：转出立即扣减源账户现金/库存及费用（现金/币费用均在源端一次性扣除）；目标账户在 `ArrivalMS` 后经 `CompleteTransfer` 才入账；在途资产计入 `Equity` 但不能交易，已扣费用不计入权益；估算器的再平衡补贴不重复计费，实际转账决定已实现成本。
- 显示深度按场所与 `AtMS` 持久消耗；仅改 Published/Received 不补充，新观测时间才补充。
- 用 decimal，不推断场所 tick/lot/提现规则（由研究输入提供）；不建模网络延迟与队列；不代表实盘。日志上限 10 万笔成交/转账标识。

## 6. 实盘边界（配对策略）

通用实盘约束与 Hub/嵌入式模式细节见截面因子文档；配对相关：

| 项 | 回测 / paper | 实盘 |
| --- | --- | --- |
| 合约 | `linear_perpetual`（执行估值仅此类与 cash 欧式期权） | 仅线性永续（`Contract/Linear/Swap`、结算币一致）；现货/反向/交割/期权拒绝；hedge 持仓拒绝 |
| 做空 | 线性永续双向开仓 | 同左 |
| 资金费 | `explicit-zero` 或 `required-stream` | 缺省/`banexg`/`hub` 走 Hub 模式：策略侧接受 `explicit-zero` 与 `required-stream`（需 `funding_source`），须显式声明；`dry_run` Hub 强制 `explicit-zero`；**仅嵌入式 banexg 绑定（非 Hub）必须 `required-stream` 且有权威资金费现金能力** |
| 价格 | Events：tick 或 1m 流；spread 类成交模型需 bid/ask | 实时订阅，需启动对账 |
| 精度 | `QuantityStep/PriceTick/MinSteps/MinNotional` 来自规范化元数据，缺失则拒绝 | 同左 |
| 成交 | 无队列优先级/部分成交/网络失败；限价单等后续报价穿越 | 真实成交，不回退 paper |
| 两腿 | 同轮同时生成，账本逐笔处理 | 异步，一腿未成交产生对冲误差 |
| 已有持仓 | - | 须先账本归因与启动对账 |

`explicit-zero` 仅为显式零费简化，永续配对用它会低估资金费成本。配置开关或自定义 provider 名不能替代真实传输、元数据、账户快照、数据版本与资金费证据。

## 7. 风险、成本与回测→实盘数量关系

### 7.1 收益与成本近似

设每单位 z 对应残差对数 σ，单对毛收益近似：
- 完整均值回归（`|z|` 从 `|Entry|` 到 `|Exit|`）：`毛收益 ≈ G·σ·(|Entry|-|Exit|)/(1+|β|)`。
- 一次往返换手约 `2G`，成本约 `2G·c`（`c`=单边费率+滑点，按腿名义计）。
- 盈亏平衡：`σ·(|Entry|-|Exit|)/(1+|β|) > 2c`。

忽略 β 漂移、资金费、冲击成本、退出价差偏离与漂移再平衡成本（2.6）；仅作筛选。

### 7.2 风险要点

- **无波动率目标**：每对名义固定 `G`。
- **止损=平仓而非保护**：`|z|>=Stop` 实现亏损并阻止再入场，不限单笔亏损。
- **无默认时间止损**：`MaxHold:0` 时不回归的价差一直占用敞口。
- **结构性断裂**（并购、退市等）：需外部失效检测，库不提供。
- **再平衡成本**：见 2.6；Events 需单独核对。
- **容量**：`research.EstimateCapacityCost(spec CapacityCostSpec{FeeRate, SlippageRate, ImpactCoefficient, MaxParticipation}, tradedNotional, adv, volatility, heldNotional, fundingRate) (CapacityCost{Participation, Fee, Slippage, Impact, Funding, Total, Feasible}, error)`：`Impact = traded·coef·vol·√(traded/adv)`；`adv>0`，`MaxParticipation∈[0,1]`（0=不限）；仅为估计。
- **账户风险限额**：Gross、保证金限额为**结算币绝对金额**，非杠杆倍数；与策略 NAV 比例 `Gross` 不互替。
- **腿不平衡**：两腿按各自步长与最小量取整（推断），小名义时改变实际 β 对冲。

### 7.3 期权：能力边界

```go
type EuropeanOptionSpec struct { Spot, Strike, Years, Volatility, Rate, DividendYield float64; Call bool }
type EuropeanOptionValue struct { Price, Delta, Gamma, Vega, Theta, Rho float64; GreeksValid bool }
func PriceEuropeanOption(s EuropeanOptionSpec) (EuropeanOptionValue, error)  // BSM 常数波动率；Vega/Rho 为单位波动率/利率；Theta 每年
func EuropeanImpliedVolatility(s EuropeanOptionSpec, price, tolerance, maxVolatility float64, maxIterations int) (float64, error)  // Years>0
func PutCallImpliedSpot(call, put, strike, years, rate, dividendYield float64) (float64, error)
func AmericanOptionPrice(s EuropeanOptionSpec, steps int) (float64, error)  // CRR 树，steps 1..5000，连续股息率，每节点提前行权
func AmericanImpliedVolatility(s EuropeanOptionSpec, premium, low, high float64, steps, iterations int, tolerance float64) (float64, error) // 0<low<high<=10，iterations<=1000，Years>0
type SurfaceOption struct { Strike, TenorDays, IV, OpenInterest float64; ExpiryMS int64; Call bool }
func OpenInterestWeightedIVSpread(quotes []SurfaceOption) (float64, error)
func VolatilitySmirk(spot float64, quotes []SurfaceOption) (float64, error)   // quotes<=10000
```

- 仅为估值/诊断工具（不含波动率微笑与离散股息），不代表能执行期权。
- 执行侧仅 paper Events 支持现金结算欧式期权：`Instrument.Valuation="cash_european_option"`，`Option{UnderlyingID, Right, Strike, ExpiryMS, UnderlyingStressMax}`；无提前行权、无实物交割。
- 到期结算由 `option_expiry_source` 提供：`SID` 为期权，`EventTime` 精确到期，`AvailableAt` 公布时间，`Values["fixing"]` 十进制字符串；超过 `UnderlyingStressMax` 被拒。
- 风险预留：多头=标记价值；空 put=`Strike×单位`；空 call=`max(StressMax-Strike,0)×单位`；不同行权价/到期不对消。
- Weights、实盘 `Trade`、Banexg 适配器均拒绝现金期权。

## 8. 示例

训练见 4.1（`EngleGranger` 的 x 为每行=[logX_i] 的 [][]float64）；`hedge.Beta/Intercept` 为静态结果，用于 `RegisterPairs` 需 2.7 自定义路径，其余参数可直接写 YAML。

```go
package pairs

import (
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/factor/statistics"
)

func init() {
	if err := runner.RegisterPairs("eth-btc", func(p *runner.Params) (runner.PairsSpec, error) {
		return runner.PairsSpec{
			Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
			Field:  "close",
			Window: p.Int("window", 120),
			Band: statistics.BandConfig{
				Entry:   p.Float("entry", 2),
				Exit:    p.Float("exit", 0.5),
				Stop:    p.Float("stop", 4),
				MaxHold: p.Int("maxHold", 0),
			},
			Gross: p.Float("gross", 0),
		}, p.Err()
	}); err != nil {
		panic(err)
	}
}
```

YAML 同 2.5，另加 `prices: {source: tick, timeframe: event, field: price}`（2.6）。
拒绝拼错参数：build 返回前加 `if u := p.Unused(); len(u) > 0 { return runner.PairsSpec{}, fmt.Errorf("unused params: %v", u) }`。

## 9. 仍不确定（未实测）

- Events 是否漂移再平衡成交；仅提高 `expiry_ms` 的成交行为；`pair-input-missing` 的真实触发场景。
- 实盘、期权、冷历史、banexg 适配器仅据源码；Patch 对配对的危害为代码确认，未端到端跑。
- Window=3–5 倍半衰期为经验法则；MacKinnon 数值来自记忆，须对照权威表。
