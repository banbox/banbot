# strat 包

strat 包提供了交易策略相关的功能定义和实现。

## 主要结构体

### TradeStrat
交易策略的核心结构体，定义了策略的基本属性和行为。

公开字段：
- `Name string` - 策略名称
- `Version int` - 策略版本号
- `WarmupNum int` - 预热所需的K线数量
- `OdBarMax int` - 回测查找未完成持仓最多向前模拟的bar数，默认500；`run_policy.order_bar_max`优先
- `MinTfScore float64` - 最小时间周期质量，默认0.75
- `WsSubs map[string]string` - websocket订阅: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
- `DrawDownExit bool` - 是否启用回撤退出
- `HedgeOff bool` -关闭合约双向持仓
- `BatchInOut bool` - 是否在主周期 `OnData(RoleMain)`/`OnBar` 后批量执行入场/出场
- `BatchInfo bool` - 是否在辅助周期 `OnData(RoleInfo)`/`OnInfoBar` 后执行批量处理
- `StakeRate float64` - 相对基础金额开单倍率
- `StopLoss float64` - 此策略打开所有订单的默认止损比率（不带杠杆）
- `StopEnterBars int` - 限价入场单超时K线数
- `OrderOnRotation string` - 品种被轮换移出后的旧任务：`open`表示有订单时仍允许继续开单；平仓与否见`pairmgr.pos_on_rotation`
- `EachMaxLong int` - 每个交易对最大做多订单数，-1表示禁用
- `EachMaxShort int` - 每个交易对最大做空订单数，-1表示禁用
- `RunTimeFrames []string` - 允许运行的时间周期；未提供时使用当前 Runtime 配置快照中的默认周期
- `RefineTF interface{}` - 策略默认的订单撮合周期，如`"5m"`、`12`、`"3-6"`；`run_policy[].refine_tf`显式配置时优先
- `Outputs []string` - 策略输出的文本文件内容，每个字符串是一行
- `Policy *config.RunPolicyConfig` - 策略运行配置
- `OnDataSubs`: 声明 K 线扩展列或自定义时序数据订阅
- `OnData`: 接收统一的 `strat.DataEvent`；事件嵌入已更新 Series/最新值的 `*strat.DataFields`，并标识主周期、辅助 K 线或自定义时序
- `OnWsData`: 接收 websocket 时序事件

没有辅助或自定义订阅时直接赋值 `OnData`。需要过滤时使用 `RouteData(DataHandlers{...})` 分别处理三类事件。`OnData` 不能与 `OnBar` 或 `OnInfoBar` 同时配置。

### StratJob
策略任务实例，负责执行具体的交易操作。

公开字段：
- `Strat *TradeStrat` - 所属策略
- `Env *ta.BarEnv` - K线环境
- `Entrys []*EnterReq` - 入场请求列表
- `Exits []*ExitReq` - 出场请求列表
- `LongOrders []*ormo.InOutOrder` - 做多订单列表
- `ShortOrders []*ormo.InOutOrder` - 做空订单列表
- `Symbol *orm.ExSymbol` - 当前运行的币种
- `TimeFrame string` - 当前运行的时间周期
- `Account string` - 当前任务所属账号
- `TPMaxs map[int64]float64` - 订单最大盈利时价格
- `OrderNum int` - 所有未完成订单数量
- `EnteredNum int` - 已完全/部分入场的订单数量
- `CheckMS int64` - 上次处理信号的时间戳，13位毫秒
- `MaxOpenLong int` - 最大开多数量，0不限制，-1禁止开多
- `MaxOpenShort int` - 最大开空数量，0不限制，-1禁止开空
- `CloseLong bool` - 是否允许平多
- `CloseShort bool` - 是否允许平空
- `ExgStopLoss bool` - 默认true，止损作为条件单提交到交易所；false时不预挂条件单，改为本地价格监控（回测与实盘均生效），`CallbackPct`追踪止损同理
- `LongSLPrice float64` - 开仓时默认做多止损价格
- `ShortSLPrice float64` - 开仓时默认做空止损价格
- `ExgTakeProfit bool` - 默认true，止盈作为条件单提交到交易所；false时改为本地价格监控
- `LongTPPrice float64` - 开仓时默认做多止盈价格：`EnterReq`未指定`TakeProfit/TakeProfitVal`时使用，指定`TakeProfits`时忽略；须高于入场价
- `ShortTPPrice float64` - 开仓时默认做空止盈价格，须低于入场价
- `IsWarmUp bool` - 当前是否处于预热状态
- `More interface{}` - 策略自定义的额外信息
- `DataHub`: 按 `(source, sid, timeframe)` 缓存最新事件和窗口数据

公开方法（开平仓与止损止盈方法的用法见[策略指南](../guide/strat_custom.md)）：
- `CanOpen(short bool) bool`、`OpenOrder(req *EnterReq) *errs.Error`、`CloseOrders(req *ExitReq) *errs.Error`
- `GetOrderNum(dirt float64) int`、`GetOrders(dirt float64) []*ormo.InOutOrder`、`Position(dirt float64, enterTag string) float64`、`PositionAvgPrice(dirt float64, enterTag string) float64`、`GetAvgCostPrice(odList ...*ormo.InOutOrder) float64`
- `SetAllStopLoss(dirt float64, args *ormo.ExitTrigger) *errs.Error`、`SetAllTakeProfit(...)`：`args`为nil取消；`dirt`为0（双向）且同时持有多空订单时panic
- `AccountEquity() float64`：账户权益（余额+浮动盈亏），预热或钱包未就绪时回退为初始资金/默认开单金额，不会为0或NaN
- `AvailableBalance() float64`：可用余额/保证金，不可用时为0
- `AccountDrawdown() float64`：相对运行以来权益峰值的回撤比例，`[0,1]`，预热期为0
- `RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub`：创建只读K线参考订阅（`symbol`为空或`_cur_`为当前品种；省略`warmupNum`取`TradeStrat.WarmupNum`）；包级`NewRefKlineSub(s, symbol, timeframe, warmupNum...)`等价
- `RefEnv(symbol, timeframe string) *ta.BarEnv`：参考行情的同步K线环境，首根K线到达前为nil
- `Data(sub *DataSub) *DataFields`：订阅数据视图，`sub.ExSymbol`为空时取当前品种
- `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot`、`OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) OpeningRangeSnapshot`、`SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot`
- `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)`、`PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)`（`nDays`为1~366）
- `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)`
- `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)`、`RBreakerLevelsBy(anchorMinutesUTC int, f1, f2, f3 float64) (RBreakerSnap, bool)`
- `ProcessRuntimeOrders()`、`ExecutionSnapshot()`、`IsWarmUpState() bool`、`OrderCounts() (orderNum, enteredNum int)`、`PendingEntryCount() int`

日内方法的锚点为UTC当日分钟数，状态按任务独立、无未来函数：区间在`Ready`/`Completed`之前`High/Low`等为0；`Applicable=false`表示周期不适用（1d及以上、与区间未对齐，或周期粗于区间且未开启`AllowSingleBarFallback`）。

### DataSub
时序数据订阅信息。

公开字段：
- `Source string` - 数据源名称；空值等同 `kline`
- `ExSymbol *orm.ExSymbol` - 绑定标的
- `TimeFrame string` - 数据源基础周期
- `WarmupNum int` - 初始化预热数量
- `Fields []string` - 需要读取的字段
- `SeriesFields []string` - 维护为`banta.Series`的字段，并自动加入读取投影；详见[自定义时序数据](../guide/custom_data.md)

### DataHub
运行时数据缓存。`Get(tf, source, sid)` 返回处理后的 `DataFields`，`AllReady()` 判断当前事件时间上应闭合的全部订阅是否已经更新。自定义数据的订阅和消费示例请参阅[自定义时序数据](../guide/custom_data.md)。

`OnBatchJobs` 仅接收主周期任务，并由 `BatchInOut` 开启；辅助周期任务进入 `OnBatchInfos`，由 `BatchInfo` 开启。

### JobEnv
在OnBatchInfos中表示某个额外品种数据的job

公开字段：
- `Job *StratJob` - 策略任务实例
- `Env *ta.BarEnv` - 指标运行环境
- `Symbol string` - 交易对名称

### BatchState / BatchMap

`BatchState` 持有单个 trader 的批量队列和回测批次时间。使用 `NewBatchState` 创建实例，`AddTask` 入队、`TakeReady(currMS, deterministic)` 取出到期批次、`PendingCount` 查询、`Reset` 清空。所有回测均使用确定性批次顺序：同一根K线内主批次 jobs 按 `BarPriority(barMS, symbol, seed)` 的中性哈希顺序排列（不是品种字母序；无有效K线时间时才按品种键排序），种子取配置 `bt_bar_order_seed`（默认0），也可用 `SetBarOrderSeed(seed uint64)` 在该实例上指定，`BarOrderSeed() (uint64, bool)` 读取。实盘批次不保证顺序；资金竞争时应自行按策略得分排序。`LegacyBatchState` 仅服务包级兼容调用。

`func BarPriority(barMS int64, symbol string, seed uint64) uint64` 返回品种在某根K线时间下的中性、可复现优先级（越小越先），可在回调中用于排序时的平局裁决。

`BatchMap` 是队列内部的一个时间周期任务组，不是进程共享的市场状态。

公开字段：
- `Map map[string]*JobEnv` - 任务映射
- `TFMSecs int64` - 时间周期毫秒数
- `ExecMS int64` - 执行批量任务的时间戳，每收到新的标的，推迟几秒；超过DelayBatchMS未收到，开始执行

### PairSub
交易对订阅信息。

公开字段：
- `Pair string` - 交易对名称
- `TimeFrame string` - 时间周期
- `WarmupNum int` - 预热数量

### EnterReq
开仓请求结构体。

公开字段：
- `CommandID string` - 稳定请求身份；事件回调可派生。幂等执行取决于订单管理器。
- `Tag string` - 入场信号（必填）
- `StratName string` - 策略名称
- `Short bool` - 是否做空
- `OrderType int` - 订单类型
- `Limit float64` - 限价单入场价格，指定时订单将作为限价单提交；与`Stop`同时设置时为Stop-Limit入场
- `Stop float64` - 入场触发价（不是止损），做多订单时价格上涨到触发价才入场（做空相反）
- `OCOGroup string` - OCO挂单组标识：组内任一挂单入场成交后，同（品种、策略、组）其余未成交挂单被撤销
- `CostRate float64` - 开仓倍率，默认按配置1倍，用于计算LegalCost
- `RiskRate float64` - 单笔止损亏损占`AccountEquity()`的比例，`[0,1]`；未指定`Amount`/`LegalCost`且有止损价时，`LegalCost = AccountEquity() × RiskRate ÷ (|入场价-止损价| ÷ 入场价)`，受杠杆（现货1倍）和`max_stake_amt`封顶；无止损时退回默认开单金额×`CostRate`
- `LegalCost float64` - 花费法币金额，指定时忽略CostRate
- `Leverage float64` - 杠杆倍数
- `Amount float64` - 入场标的数量，由LegalCost和price计算
- `StopLossVal float64` - 入场价格到止损价格的距离，用于计算StopLoss
- `StopLoss float64` - 止损触发价格；默认在交易所提交止损条件单，`ExgStopLoss=false`时仅本地监控
- `StopLossLimit float64` - 止损触发后的限价（0为市价）；`StopLoss`为0时同时作为触发价
- `StopLossRate float64` - 止损退出比例，0表示全部退出，需介于(0,1]之间
- `StopLossTag string` - 止损原因
- `TakeProfitVal float64` - 入场价格到止盈价格的距离，用于计算TakeProfit
- `TakeProfit float64` - 止盈触发价格；默认在交易所提交止盈单，`ExgTakeProfit=false`时仅本地监控
- `TakeProfitLimit float64` - 止盈触发后的限价（0为市价）；`TakeProfit`为0时同时作为触发价
- `TakeProfitRate float64` - 止盈退出比率，0表示全部退出，需介于(0,1]之间
- `TakeProfitTag string` - 止盈原因
- `TakeProfits []TakeProfitTarget` - 多级阶梯止盈；不能与`TakeProfit`/`TakeProfitVal`/`TakeProfitLimit`同时使用，设置后`LongTPPrice/ShortTPPrice`不再生效
- `StopBars int` - 入场限价单超过多少个bar未成交则取消
- `ClientID string` - 用于设置提交到交易所的ClientOrderID尾部部分。
- `ActivationPrice float64` - 追踪止损激活价格，0为立即生效；仅`CallbackPct>0`时有效
- `CallbackPct float64` - 追踪止损回撤百分比（单位%，1.5即1.5%），0关闭，非零须在`[0.05, 50]`内；实盘优先使用交易所原生追踪单，不支持或被拒绝时降级为本地监控
- `Infos map[string]string` - 用于保存到订单的额外信息
- `Log bool` - 失败时自动记录日志

`Stop` 与 `Limit` 同时设置为 Stop-Limit 入场：价格触及 `Stop` 后以 `Limit` 挂限价单，`Stop` 已被当前价越过时直接按 `Limit` 挂限价单。`OrderType` 为 `core.OrderTypeMarket` 时不能设置 `Limit`。`StopLossVal`/`TakeProfitVal` 是价格差而非比例。预热期间不会入队；返回 nil 不代表已经成交。`CloseOrders` 仅排队；特定方向的有利侧限价退出会转成订单止损限价，而不是立即成交。

### TakeProfitTarget
阶梯止盈的一级。

公开字段：
- `Price float64` / `Val float64` / `Rate float64` - 三选一（须大于0）：绝对触发价；距入场成交均价的价差；相对入场成交均价的比例。同时给出时`Price`优先，其次`Val`；`Price`须位于盈利方向
- `Limit float64` - 触发后的限价，0为市价
- `ExitRate float64` - 本级平掉初始仓位的比例，`(0,1]`；仅一级且为0时按1处理；各级之和不得超过1
- `MoveSLToBreakeven bool` - 本级成交后，把剩余仓位的止损收紧到不差于入场价（不放宽更紧的止损）
- `Tag string` - 本级退出原因

各级按距入场价由近到远依次触发。

### ExitReq
平仓请求结构体。

公开字段：
- `CommandID string` - 稳定请求身份；相同ID和内容的请求重放时不会重复提交
- `Tag string` - 退出信号
- `StratName string` - 策略名称
- `EnterTag string` - 只退出入场信号为EnterTag的订单
- `Dirt int` - 方向(core.OdDirt*)，多/空/双向；零值即双向。同时持有多空订单且设置`Limit`时必须显式指定
- `OrderType int` - 订单类型
- `Limit float64` - 限价单退出价格，指定时订单将作为限价单提交
- `ExitRate float64` - 退出比率，默认100%即所有订单全部退出
- `Amount float64` - 要退出的标的数量，指定时ExitRate无效
- `OrderID int64` - 只退出指定订单
- `UnFillOnly bool` - True时只退出尚未入场的部分
- `FilledOnly bool` - True时只退出已入场的订单
- `Force bool` - 是否强制退出
- `Log bool` - 失败时自动记录日志

## 策略管理相关方法

### New
创建新的交易策略实例。

参数：
- `pol *config.RunPolicyConfig` - 策略运行配置

返回：
- `*TradeStrat` - 交易策略实例

### Get
根据交易对和策略ID获取策略实例。

参数：
- `pair string` - 交易对名称
- `stratID string` - 策略ID

返回：
- `*TradeStrat` - 交易策略实例，如果不存在则返回 nil

### GetStratPerf
获取策略性能配置。

参数：
- `pair string` - 交易对名称
- `strat string` - 策略名称

返回：
- `*config.StratPerfConfig` - 策略性能配置

### AddStratGroup
添加策略组。

参数：
- `group string` - 策略组名称
- `items map[string]FuncMakeStrat` - 策略创建函数映射

## 订单管理相关方法

### GetJobs
获取指定账户的所有策略任务。

参数：
- `account string` - 账户名称

返回：
- `map[string]map[string]*StratJob` - 按交易对和策略分组的任务映射

### GetInfoJobs
获取指定账户的信息策略任务。

参数：
- `account string` - 账户名称

返回：
- `map[string]map[string]*StratJob` - 按交易对和策略分组的信息任务映射

### AddOdSub
添加订单变更订阅。

参数：
- `acc string` - 账户名称
- `cb FnOdChange` - 订单变更回调函数

### FireOdChange
触发订单变更事件。

参数：
- `acc string` - 账户名称
- `od *ormo.InOutOrder` - 订单对象
- `evt int` - 事件类型

## 性能计算相关方法

### CalcJobScores
计算策略任务得分。

参数：
- `pair string` - 交易对名称
- `tf string` - 时间周期
- `stgy string` - 策略名称

返回：
- `*errs.Error` - 错误信息

### CalcJobPerfs
计算策略任务性能。

参数：
- `cfg *config.StratPerfConfig` - 性能配置
- `p *core.PerfSta` - 性能统计对象
- `perfs []*core.JobPerf` - 性能数据列表

### CalcDrawDownExitRate
计算回撤退出比率。

参数：
- `maxChg float64` - 最大变化率

返回：
- `float64` - 计算得到的退出比率

## 系统管理相关方法

### LoadStratJobs
加载策略任务。

参数：
- `pairs []string` - 交易对列表
- `tfScores map[string]map[string]float64` - 时间周期得分映射

返回：
- `map[string]map[string]int` - 任务状态映射
- `map[string][]*ormo.InOutOrder` - 订单映射
- `*errs.Error` - 错误信息

### ExitStratJobs
退出所有策略任务。

## 因子引擎集成

TradeStrat 是时序域，图和组合在 factor/runner；DataHub rawMap 的 NULL/缺失语义与数值视图并存。

[因子 API](factor.md) / [指南](../guide/factor.md)

## 回调与任务方法

```go
OnStartUp     func(s *StratJob)
OnDataSubs    func(s *StratJob) []*DataSub
OnData        func(s *StratJob, data DataEvent)
OnSymbols     func(items []string) []string
OnCheckExit   func(s *StratJob, od *ormo.InOutOrder) *ExitReq
OnOrderChange func(s *StratJob, od *ormo.InOutOrder, chgType int)
OnBatchJobs   func(jobs []*StratJob)
OnBatchInfos  func(tf string, jobs map[string]*JobEnv)
OnWsData      func(s *StratJob, evt *orm.DataSeries)
OnPostApi     FnOnPostApi
OnShutDown    func(s *StratJob)
OnStratExit   func()
```

兼容回调：`OnBar func(s *StratJob)`、`OnInfoBar func(s *StratJob, e *ta.BarEnv, pair, tf string)`、`OnPairInfos func(s *StratJob) []*PairSub`、`OnWsTrades`、`OnWsDepth`、`OnWsKline`；另有`GetDrawDownExitRate CalcDDExitRate`和`PickTimeFrame PickTimeFrameFunc`。

以上为回调字段签名，省略其他配置字段。`OnData` 与 `OnBar`/`OnInfoBar` 互斥，混用会 panic。用 `RouteData(DataHandlers{Main: ..., Info: ..., Custom: ...})` 区分事件；事件的 `DataFields` 已更新。`DataSub` 是 `orm.Subscription` 的别名。

`job.Data(sub)` 返回订阅视图，`DataHub.Get(tf, source, sid)` 返回相同身份的数据；未到达时应检查 nil。`RawValue(name) (any, bool)` 区分缺失与显式 NULL，`Series(name)` 提供数值指标窗口。不要把自定义字段统一转成 float64。

`job.OpenOrder(req)`、`CloseOrders(req)`、`SetAllStopLoss(dirt, trigger)`、`SetAllTakeProfit(dirt, trigger)` 均返回 `*errs.Error`。`ProcessRuntimeOrders()` 返回 `(entered, exited []*ormo.InOutOrder, err *errs.Error, bound bool)`，只处理当前 job 的请求；bound=false 表示未绑定显式 Runtime。不要直接追加 Entrys/Exits。跨事件读取执行状态可用 `ExecutionSnapshot()`，其中订单指针仍是借用。

`OnBatchJobs` 中的开平仓请求在回调后由框架自动提交（预热期除外）；`OnBatchInfos` 中的开平仓请求不会自动提交，只记录WARN。

`RegisterStrategy(name, factory)` 注册完整名称，`AddStratGroup` 注册分组。运行中任务从 `rt.Strategies.CollectJobs()` 或 `LookupJob(account, pairTF, strategy)` 获取；包级 `New/Get/GetJobs/GetInfoJobs/AddOdSub/FireOdChange/LoadStratJobs/ExitStratJobs` 是兼容入口，不能查询独立 Runtime 的状态。动态修改品种时在 `PairUpdateReq.StrategyState` 传入所属 `rt.Strategies`；参见[策略指南](../guide/strat_custom.md)。

### UpdatePairs
`func (s *TradeStrat) UpdatePairs(req PairUpdateReq) (*PairUpdateResult, *errs.Error)` 在运行中增删品种。`PairUpdateReq{Strat, StrategyState, Core, Exchange, Add, Remove, CloseOnRemove, ForceAdd, Reason}`；`Strat`为空时取接收者，不等于接收者则返回错误。`PairUpdateResult{Added, Removed, Skipped, ExitOrders, Warnings}`。仅回测可用：实盘与 dry_run 的 execution 路径中品种在启动时固定，调用返回 `pair updates are not available in this runtime…` 错误。

### 参数校验
策略构造函数中可用`pol.Require(cond bool, msg string)`和`pol.Requiref(cond bool, format string, args ...any)`校验参数；不满足时该策略构造失败并按BadConfig（配置错误）报告，不会被缓存，普通运行时错误不会被改写为配置错误。
