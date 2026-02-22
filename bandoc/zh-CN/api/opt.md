# opt 包

opt 包提供了策略优化相关的功能。

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

### NewBackTest
创建一个新的回测实例。

参数：
- `isOpt bool` - 是否为超参数优化模式
- `outDir string` - 输出目录路径

返回：
- `*BackTest` - 回测实例指针

### RunBTOverOpt
基于持续调参的回测模式，接近实盘情况，避免使用未来信息调参回测。

参数：
- `args *config.CmdArgs` - 命令行参数配置

返回：
- `*errs.Error` - 错误信息

### RunRollBTPicker
执行滚动回测选股器。

参数：
- `args *config.CmdArgs` - 命令行参数配置

返回：
- `*errs.Error` - 错误信息

### RunOptimize
执行策略参数优化。

参数：
- `args *config.CmdArgs` - 命令行参数配置

返回：
- `*errs.Error` - 错误信息

### CollectOptLog
收集并分析优化日志。

参数：
- `args *config.CmdArgs` - 命令行参数配置

返回：
- `*errs.Error` - 错误信息

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

### DumpLineGraph
生成折线图并保存。

参数：
- `path string` - 输出文件路径
- `title string` - 图表标题
- `label []string` - 标签列表
- `prec float64` - 精度
- `tplData []byte` - 模板数据
- `items []*ChartDs` - 图表数据集

返回：
- `*errs.Error` - 错误信息

### CompareExgBTOrders
比较交易所回测订单。

参数：
- `args []string` - 命令行参数列表
