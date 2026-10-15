# opt 包

opt 包提供了策略优化相关的功能。

回测和优化运行器通过 `NewBackTestLiteWithRuntimeDeps`、`NewBackTestWithRuntimeDeps` 接收一个 Runtime 的依赖。每个运行器使用自己的时钟、策略、订单、钱包和数据投影；优化过程中的每轮执行也按 Runtime 生命周期创建和释放。

## 主要结构体

### BackTest
回测实例结构体。

字段：
- `Trader biz.Trader` - 交易者接口实现
- `BTResult *BTResult` - 回测结果
- `lastDumpMs int64` - 上一次保存回测状态的时间戳
- `dp *data.HistProvider` - 历史数据提供者
- `isOpt bool` - 是否为超参数优化模式
- `PBar *utils.StagedPrg` - 进度条

### BTResult
回测结果结构体。

字段：
- `MaxOpenOrders int` - 最大同时持仓订单数
- `MinReal float64` - 最小资产
- `MaxReal float64` - 最大资产
- `MaxDrawDownPct float64` - 最大回撤百分比
- `ShowDrawDownPct float64` - 显示的最大回撤百分比
- `MaxDrawDownVal float64` - 最大回撤金额
- `ShowDrawDownVal float64` - 显示的最大回撤金额
- `BarNum int` - K线数量
- `TimeNum int` - 时间周期数
- `OrderNum int` - 订单数量
- `Plots *PlotData` - 绘图数据
- `StartMS int64` - 开始时间戳(毫秒)
- `EndMS int64` - 结束时间戳(毫秒)
- `PlotEvery int` - 绘图间隔
- `TotalInvest float64` - 总投资金额
- `OutDir string` - 输出目录
- `TotProfit float64` - 总盈利
- `TotCost float64` - 总成本
- `TotFee float64` - 总手续费
- `TotProfitPct float64` - 总收益率
- `WinRatePct float64` - 胜率
- `SharpeRatio float64` - 夏普比率
- `SortinoRatio float64` - 索提诺比率

### PlotData
绘图数据结构体。

字段：
- `Labels []string` - 时间标签
- `OdNum []int` - 订单数量
- `JobNum []int` - 任务数量
- `Real []float64` - 实际资产
- `Available []float64` - 可用资产
- `Profit []float64` - 已实现盈利
- `UnrealizedPOL []float64` - 未实现盈亏
- `WithDraw []float64` - 提现金额

### RowPart
回测统计行数据结构体。

字段：
- `WinCount int` - 盈利订单数
- `OrderNum int` - 订单总数
- `ProfitSum float64` - 总盈利金额
- `ProfitPctSum float64` - 总盈利率
- `CostSum float64` - 总成本
- `Durations []int` - 持仓时长列表
- `Orders []*InOutOrder` - 订单列表
- `Sharpe float64` - 夏普比率
- `Sortino float64` - 索提诺比率

## 主要功能

### NewBackTestWithRuntimeDeps

实际签名为 NewBackTestWithRuntimeDeps(deps biz.RuntimeDeps, isOpt bool, outDir string) (*BackTest, *errs.Error)。NewBackTest 已删除；传入同一 Runtime 的完整依赖，缺失状态报错。轻量事件回放使用 NewBackTestLiteWithRuntimeDeps。

### RunBTOverOpt

实际签名：RunBTOverOpt(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. 入口注入配置快照和隔离回测工厂，不能仅传 CmdArgs 调用；该路径是 TS 优化/报告，不是 factor/mixed 超参搜索。

### RunRollBTPicker

实际签名：RunRollBTPicker(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. 入口注入配置快照和隔离回测工厂，不能仅传 CmdArgs 调用；该路径是 TS 优化/报告，不是 factor/mixed 超参搜索。

### RunOptimize

实际签名：RunOptimize(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. 入口注入配置快照和隔离回测工厂，不能仅传 CmdArgs 调用；该路径是 TS 优化/报告，不是 factor/mixed 超参搜索。

### CollectOptLog

实际签名：CollectOptLog(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. 入口注入配置快照和隔离回测工厂，不能仅传 CmdArgs 调用；该路径是 TS 优化/报告，不是 factor/mixed 超参搜索。

### NewBTResult
创建新的回测结果实例。

返回：
- `*BTResult` - 回测结果实例指针

### AvgGoodDesc
计算指定收益率范围内的优化结果平均值。

参数：
- `items []*OptInfo` - 优化信息列表
- `startRate float64` - 起始收益率
- `endRate float64` - 结束收益率

返回：
- `*OptInfo` - 平均优化信息

### DescGroups
将优化结果按照收益率分组。

参数：
- `items []*OptInfo` - 优化信息列表

返回：
- `[]*OptInfo, []*OptInfo` - 好组和坏组的优化信息列表

### CompareExgBTOrders
比较交易所回测订单。

参数：
- `args []string` - 命令行参数列表

## 因子引擎集成

优化工厂保持独立 Runtime；因子研究 JSON lines/账户审计 Gob 不等同旧 orders.gob。

[因子 API](factor.md) / [指南](../guide/factor.md)


## 工厂、报告与资源

BacktestFactory 的签名是 func(snapshot *config.Snapshot, isOpt bool, outDir string) (*BackTest, func(), *errs.Error)，cleanup 只释放本轮拥有状态。派生 Snapshot 复制时间范围、pairs、policies，账户/钱包不能与另一轮共享可变配置。报告通过 NewReportDeps(biz.RuntimeDeps) 绑定订单、clock、symbols、storage 和 logger，不安装 globals；DumpLineGraph 已不是公共 opt API。见[runtime](runtime.md)。
