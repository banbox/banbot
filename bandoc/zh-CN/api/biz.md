# biz 包

biz 包提供了业务逻辑层的功能实现。

## 任务依赖

当前交易器通过 `biz.RuntimeDeps` 接收一个 Runtime 的核心状态、时钟、市场、策略、订单、钱包、配置、存储与交易所依赖；`NewTraderWithRuntimeDeps` 不会回退查找缺失状态。订单处理和钱包等可变业务状态因此归属创建它们的任务。

`SetupComs` 与不带 RuntimeDeps 的旧构造路径仍用于兼容，不能用于需要和其他任务并行的新增执行流程。

## 主要结构体

### LiveOrderMgr
实时订单管理器,用于管理实盘交易中的订单。继承自 `OrderMgr`。

### OrderMgr
订单管理器基类,提供订单管理的基础功能。

主要字段:
- `Account`: 账户名称
- `BarMS`: 当前K线时间戳(毫秒)

### IOrderMgr
订单管理器接口,定义了订单管理的基本方法。

主要方法:
- `ProcessOrders`: 处理订单请求
- `EnterOrder`: 处理入场订单
- `ExitOpenOrders`: 处理退出订单
- `ExitOrder`: 处理单个订单的退出
- `UpdateByDataSeries`: 根据K线更新订单状态
- `OnEnvEnd`: 环境结束时的处理
- `CleanUp`: 清理资源

### IOrderMgrLive
实时订单管理器接口,继承自 `IOrderMgr`。

额外方法:
- `SyncExgOrders`: 同步交易所订单
- `WatchMyTrades`: 监听账户交易
- `TrialUnMatchesForever`: 持续监听未匹配交易
- `ConsumeOrderQueue`: 消费订单队列

### ItemWallet
单个币种的钱包。

主要字段:
- `Coin`: 币种代码,非交易对
- `Available`: 可用余额
- `Pendings`: 买入卖出时锁定金额,键可以是订单id
- `Frozens`: 空单等长期冻结金额,键可以是订单id
- `UnrealizedPOL`: 此币的公共未实现盈亏,合约用到,可抵扣其他订单保证金
- `UsedUPol`: 已占用的未实现盈亏(用作其他订单的保证金)
- `Withdraw`: 从余额提现的,不会用于交易

### BanWallets
账户钱包管理器。

主要字段:
- `Items`: 币种到钱包的映射
- `Account`: 账户名称
- `IsWatch`: 是否正在监听余额变化

### Trader

`NewTraderWithRuntimeDeps(deps RuntimeDeps) (Trader, *errs.Error)` 构造交易器。`FeedDataSeries(evt *orm.DataSeries) *errs.Error` 驱动策略、指标与订单处理，`FeedSeries` 是兼容别名。DataSeries.Values 保留扩展字段及 NULL/类型；不要转换成固定 OHLCV 再喂入。

## 公开方法

### SetupComs（兼容接口）
初始化基础组件。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 设置错误打印函数
- 创建上下文和取消函数
- 初始化数据目录
- 加载配置文件
- 设置日志系统
- 初始化核心组件、交易所、ORM和商品模块
- 主要用于系统启动时的基础设施初始化

### SetupComsExg（兼容接口）
初始化交易所相关的基础组件。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 调用 `SetupComs` 完成基础初始化
- 初始化交易所的 ORM 模块
- 主要用于需要交易所功能时的初始化

### InitOdSubs
初始化订单订阅。

实现细节：
- 收集需要监听订单变化的策略
- 为每个账户添加订单订阅回调
- 在订单状态变化时通知相关策略
- 用于实时监控订单状态变化

### AddBatchJob
添加批量任务。

参数：
- `account`: string - 账户名
- `tf`: string - 时间周期
- `job`: *strat.StratJob - 策略任务
- `infoEnv`: *ta.BarEnv - 辅助周期环境；主周期传 nil

### TryFireBatches

`TryFireBatches(currMS int64, isWarmUp bool) int`
尝试触发批量任务。

参数：
- `currMS`: int64 - 当前毫秒时间戳

返回：
- `int` - 尚待处理的任务数量

### InitDataDir
初始化数据目录。

返回：
- `*errs.Error` - 错误信息

### GetOdMgr
获取订单管理器。

参数：
- `account`: string - 账户名

返回：
- `IOrderMgr` - 订单管理器接口

### GetAllOdMgr
获取所有订单管理器。

返回：
- `map[string]IOrderMgr` - 账户名到订单管理器的映射

### GetLiveOdMgr
获取实时订单管理器。

参数：
- `account`: string - 账户名

返回：
- `*LiveOrderMgr` - 实时订单管理器

### CleanUpOdMgr
清理订单管理器。

返回：
- `*errs.Error` - 错误信息

### InitLiveOrderMgr
初始化实时订单管理器。

参数：
- `callBack`: func(od *ormo.InOutOrder, isEnter bool) - 订单回调函数

实现细节：
- 为每个账户创建实时订单管理器
- 设置订单回调函数
- 用于管理实时交易订单

### InitLocalOrderMgr
初始化本地订单管理器。

参数：
- `callBack`: func(od *ormo.InOutOrder, isEnter bool) - 订单回调函数
- `showLog`: bool - 是否显示日志

### VerifyTriggerOds
验证触发订单。

### StartLiveOdMgr
启动实时订单管理器。

### InitFakeWallets
初始化模拟钱包。

参数：
- `symbols`: ...string - 交易对符号列表

### GetWallets
获取钱包。

参数：
- `account`: string - 账户名

返回：
- `*BanWallets` - 钱包对象

### WatchLiveBalances
监控实时余额。

## 因子引擎集成

Trader.FeedDataSeries 驱动时序 jobs；共享账户桥接是 execution 投影，不是第二套账户。AccountSink 投递独立 Full/Patch 目标。

[因子 API](factor.md) / [指南](../guide/factor.md)


## 策略订单处理与账户状态

策略通常只调用 `job.OpenOrder`/`CloseOrders`。它们排队请求，订单管理器负责校验、撮合或发送。显式处理使用 `job.ProcessRuntimeOrders()`，不要在独立 Runtime 中按账户名查询包级 manager。

```go
// IOrderMgr method signatures:
ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error)
ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error)
ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error)
UpdateByDataSeries(allOpens []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error
OnEnvEnd(evt *orm.DataSeries) *errs.Error
```

嵌入程序用 `GetOdMgrWithState(rt.Trading, account)`、`GetAllOdMgrWithState(rt.Trading)`、`GetLiveOdMgrWithState(rt.Trading, account)` 和 `rt.Trading.Wallet(account)` 获取所属任务状态。检查 nil；禁用的账户不保证有 manager。包级同名无 State 函数只读兼容注册表。

InitOdSubs、InitLiveOrderMgr、InitLocalOrderMgr、InitFakeWallets 和 StartLiveOdMgr 是装配用的兼容入口，新增执行流程使用对应 WithRuntimeDeps 入口。WatchLiveBalances 接受 WalletRuntimeLifecycle；标准 live 入口负责绑定所属 Runtime 并管理停止和等待。InitLocalOrderMgr 还接受 `stops ...func()`。Runtime.Accounts/AccountsMu 是 manager、wallet、job 共用的可变执行配置，Config 快照只读。

实盘钱包以交易所余额为准；本地回测/模拟钱包不能证明真实可成交余额。仅在兼容 `LiveOrderMgr` 直接发送分支中，CreateOrder 错误（包括超时）会触发本地失败关单，后续对账可能恢复实际订单。这不代表默认 Hub/共享账户执行路径：共享执行对结果不明确的发送保留 `Unknown` 等状态并阻止普通增减仓，不允许用新命令乱重试；应使用原命令 ID 与一致内容恢复并核实交易所证据。详见[共享账户与 Hub](../guide/execution.md)。

见 [Runtime API](runtime.md) 与[策略指南](../guide/strat_custom.md)。
