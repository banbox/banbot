# opt 包

`opt` 提供时序回测、超参优化、滚动优化和报告工具。因子/混合 `backtest` 由统一入口另行装配，不能直接复用此处的时序优化 API 搜索因子参数。使用命令行时参见[回测](../guide/backtest.md)、[优化](../guide/hyperopt.md)及[滚动回测](../guide/roll_btopt.md)。

## 回测实例

```go
func NewBackTestWithRuntimeDeps(
    deps biz.RuntimeDeps, isOpt bool, outDir string,
) (*BackTest, *errs.Error)

func NewBackTestLiteWithRuntimeDeps(
    deps biz.RuntimeDeps, isOpt bool, onBar data.FnDataSeries,
    getEnd data.FnGetInt64, pBar *utils.StagedPrg,
) (*BackTestLite, *errs.Error)
```

使用同一 Runtime 的完整依赖；缺少必要状态时报错。旧 `NewBackTest` 已删除。`BackTest` 公开 `Init()`、`Run()`、`RunContext(context.Context)` 和 `FeedDataSeries(*orm.DataSeries)`；轻量 `FeedDataSeries` 返回 bool。不要把私有历史 provider、进度字段当作可构造参数。

每个运行器绑定自己的时钟、订单、钱包和数据。`orm.DataSeries.Values map[string]any` 传递扩展字段及类型/NULL 语义，不能将输入收窄为固定 OHLCV 结构。

## 优化与工厂

```go
type BacktestFactory func(
    snapshot *config.Snapshot, isOpt bool, outDir string,
) (*BackTest, func(), *errs.Error)

func RunOptimize(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func RunBTOverOpt(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func RunRollBTPicker(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func CollectOptLog(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
```

四个入口均需注入 Snapshot 和隔离工厂，不支持只传 `CmdArgs` 的旧调用。工厂为每轮创建独立 Runtime，返回 cleanup；运行器完成后调用 cleanup，失败也必须释放资源。派生 Snapshot 复制时间范围、pairs 和 policies，不能让不同试验共享可变账户/钱包状态。

`RunOptimize` 使用 `OutPath` 日志文件并打印所选策略 YAML；`RunBTOverOpt` 结转窗口间余额并积累报告；`RunRollBTPicker` 输出各窗口挑选器分数与排名。`CollectOptLog` 单文件排序写回，目录递归收集 `.log`；见[优化指南](../guide/hyperopt.md)的文件边界和输出细节。

`OptInfo` 保存 `Params`、`Score`、方向、试验 ID 和 `BTResult`；`OptGroup` 关联来源策略、周期和品种。`DescGroups(items)` 按分数分为非负与负数组；`AvgGoodDesc(items, startRate, endRate)` 对所选排名区间平均参数，无非负分数时回退负分数组。它们不是按原始收益率分位筛选。

## 报告与结果字段

`NewBTResult() *BTResult` 创建结果结构；报告依赖通过 `NewReportDeps(deps biz.RuntimeDeps) *ReportDeps` 绑定同一任务的配置、订单、钱包、时钟、存储与日志，不能靠安装全局配置补齐。`DumpLineGraph` 已不是公开 opt API。

| Go 字段 / JSON 字段 | 含义 |
| --- | --- |
| `TotProfit` / `totProfit`、`TotProfitPct` / `totProfitPct` | 总收益金额、百分比 |
| `WinRatePct` / `winRatePct` | 交易胜率百分数，不是 0–1 比例 |
| `MaxDrawDownPct` / `maxDrawDownPct` | 全过程权益最大回撤百分比 |
| `ShowDrawDownPct` / `showDrawDownPct` | 图表采样口径回撤，可能与全过程不同 |
| `SharpeRatio`、`SortinoRatio` | 收益风险比率 |
| `UlcerIndex`、`UlcerPerformanceIndex` | 指针指标，无法定义时可为 null |
| `DailyEquity` / `dailyEquity` | `[UTC 日末毫秒, 权益]`，逐日不抽样 |
| `GapExits`、`DelistExits` | 数据空洞/退市强平单数 |
| `GapExitPnlShare`、`DelistExitPnlShare` | 对应退出的绝对利润和占全部订单绝对利润和的比例 |
| `Liquidated` 等 | 爆仓/破产时的可选证据字段，见[回测指南](../guide/backtest.md) |

`PlotData` 包含 `Labels`、`OdNum`、`JobNum`、`Real`、`Available`、`Profit`、`UnrealizedPOL`、`WithDraw` 和 `More`。`RowPart` 保存分组订单计数、收益、成本和风险指标；`Orders` 类型为 `[]*ormo.InOutOrder`，不序列化到 JSON。

`BTResult.Score()` 用总收益百分比与展示回撤计算优化分数；`loss` 是其负值。普通回测写不出 `detail.json` 会返回错误并令命令非零退出；不可将缺失报告当成成功。爆仓按正常回测终止输出结果，不能只看退出码判断收益。

## 工具接口边界

订单对比优先用 CLI `tool cmp-orders` 或 `CompareExgBTOrdersWithRuntimeDeps(options CompareOrdersOptions, deps CompareOrdersDeps) error`；旧 `CompareExgBTOrders(args []string)` 不是当前 CLI 装配入口。`RegisterFactor(name string, factor FactorFunc)` / `GetFactor` / `SnapshotFactors` 管理 `tool bt-factor` 的滚动筛选函数，与原生 factor runner 不同。

因子回测的 `events.jsonl`、`resolved.json`、`run.json` 和账户审计文件不是时序 `orders.gob`，不能据后者是否存在判断因子任务成败。见[因子 API](./factor.md)与 [Runtime API](./runtime.md)。
