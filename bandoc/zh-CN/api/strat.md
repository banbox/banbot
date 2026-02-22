# strat 包

strat 包提供了交易策略相关的功能定义和实现。

## 主要结构体

### TradeStrat
交易策略的核心结构体，定义了策略的基本属性和行为。

公开字段：
- `Name string` - 策略名称
- `Version int` - 策略版本号
- `WarmupNum int` - 预热所需的K线数量
- `MinTfScore float64` - 最小时间周期质量，默认0.75
- `WsSubs map[string]string` - websocket订阅: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
- `DrawDownExit bool` - 是否启用回撤退出
- `HedgeOff bool` -关闭合约双向持仓
- `BatchInOut bool` - 是否批量执行入场/出场
- `BatchInfo bool` - 是否对OnInfoBar后执行批量处理
- `StakeRate float64` - 相对基础金额开单倍率
- `StopLoss float64` - 此策略打开所有订单的默认止损比率（不带杠杆）
- `StopEnterBars int` - 限价入场单超时K线数
- `EachMaxLong int` - 每个交易对最大做多订单数，-1表示禁用
- `EachMaxShort int` - 每个交易对最大做空订单数，-1表示禁用
- `AllowTFs []string` - 允许运行的时间周期，不提供时使用全局配置
- `Outputs []string` - 策略输出的文本文件内容，每个字符串是一行
- `Policy *config.RunPolicyConfig` - 策略运行配置

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
- `ExgStopLoss bool` - 是否允许交易所止损
- `LongSLPrice float64` - 开仓时默认做多止损价格
- `ShortSLPrice float64` - 开仓时默认做空止损价格
- `ExgTakeProfit bool` - 是否允许交易所止盈
- `LongTPPrice float64` - 开仓时默认做多止盈价格
- `ShortTPPrice float64` - 开仓时默认做空止盈价格
- `IsWarmUp bool` - 当前是否处于预热状态
- `More interface{}` - 策略自定义的额外信息

### JobEnv
在OnBatchInfos中表示某个额外品种数据的job

公开字段：
- `Job *StratJob` - 策略任务实例
- `Env *ta.BarEnv` - 指标运行环境
- `Symbol string` - 交易对名称

### BatchMap
当前交易所-市场-时间周期下，所有标的的批量执行任务池。

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
- `Tag string` - 入场信号
- `StgyName string` - 策略名称
- `Short bool` - 是否做空
- `OrderType int` - 订单类型
- `Limit float64` - 限价单入场价格，指定时订单将作为限价单提交
- `Stop float64` - 止损(触发价格)，做多订单时价格上涨到触发价格才入场（做空相反）
- `CostRate float64` - 开仓倍率，默认按配置1倍，用于计算LegalCost
- `LegalCost float64` - 花费法币金额，指定时忽略CostRate
- `Leverage float64` - 杠杆倍数
- `Amount float64` - 入场标的数量，由LegalCost和price计算
- `StopLossVal float64` - 入场价格到止损价格的距离，用于计算StopLoss
- `StopLoss float64` - 止损触发价格，不为空时在交易所提交一个止损单
- `StopLossLimit float64` - 止损限制价格，不提供使用StopLoss
- `StopLossRate float64` - 止损退出比例，0表示全部退出，需介于(0,1]之间
- `StopLossTag string` - 止损原因
- `TakeProfitVal float64` - 入场价格到止盈价格的距离，用于计算TakeProfit
- `TakeProfit float64` - 止盈触发价格，不为空时在交易所提交一个止盈单
- `TakeProfitLimit float64` - 止盈限制价格，不提供使用TakeProfit
- `TakeProfitRate float64` - 止盈退出比率，0表示全部退出，需介于(0,1]之间
- `TakeProfitTag string` - 止盈原因
- `StopBars int` - 入场限价单超过多少个bar未成交则取消
- `ClientID string` - 用于设置提交到交易所的ClientOrderID尾部部分。
- `Infos map[string]string` - 用于保存到订单的额外信息

### ExitReq
平仓请求结构体。

公开字段：
- `Tag string` - 退出信号
- `StgyName string` - 策略名称
- `EnterTag string` - 只退出入场信号为EnterTag的订单
- `Dirt int` - 方向(core.OdDirt*)，多/空/双向
- `OrderType int` - 订单类型
- `Limit float64` - 限价单退出价格，指定时订单将作为限价单提交
- `ExitRate float64` - 退出比率，默认100%即所有订单全部退出
- `Amount float64` - 要退出的标的数量，指定时ExitRate无效
- `OrderID int64` - 只退出指定订单
- `UnFillOnly bool` - True时只退出尚未入场的部分
- `FilledOnly bool` - True时只退出已入场的订单
- `Force bool` - 是否强制退出

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
