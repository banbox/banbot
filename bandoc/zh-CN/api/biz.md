# biz 包

biz 包提供了业务逻辑层的功能实现。

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
- `UpdateByBar`: 根据K线更新订单状态
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
交易管理器,负责处理交易策略和订单执行。

主要方法:
- `OnEnvJobs`: 处理环境任务
- `FeedKline`: 处理K线数据
- `ExecOrders`: 执行订单

## 公开方法

### SetupComs
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

### SetupComsExg
初始化交易所相关的基础组件。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 调用 `SetupComs` 完成基础初始化
- 初始化交易所的 ORM 模块
- 主要用于需要交易所功能时的初始化

### LoadRefreshPairs
加载并刷新交易对信息。

参数：
- `dp`: data.IProvider - 数据提供者接口
- `showLog`: bool - 是否显示日志
- `pBar`: *utils.StagedPrg - 进度条对象

返回：
- `*errs.Error` - 错误信息

实现细节：
- 刷新交易对列表
- 计算交易对时间周期得分
- 加载策略任务
- 处理未完成订单
- 订阅需要预热的交易对
- 用于系统启动或定期更新交易对信息时

### AutoRefreshPairs
自动刷新交易对信息。

参数：
- `dp`: data.IProvider - 数据提供者接口
- `showLog`: bool - 是否显示日志

实现细节：
- 自动调用 `LoadRefreshPairs`
- 处理刷新失败的错误日志
- 用于定时自动刷新交易对信息

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
- `isInfo`: bool - 是否为信息类型

### TryFireBatches
尝试触发批量任务。

参数：
- `currMS`: int64 - 当前毫秒时间戳

返回：
- `int` - 触发的任务数量

### ResetVars
重置变量。

实现细节：
- 重置各种全局映射和变量
- 包括订单管理器、钱包、K线环境等
- 用于系统重启或重置时清理状态

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

### RunDataServer
运行数据服务器。

参数：
- `args`: *config.CmdArgs - 命令行参数

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

### LoadZipKline
从ZIP文件加载K线数据。

参数：
- `inPath`: string - 输入路径
- `fid`: int - 文件ID
- `file`: *zip.File - ZIP文件对象
- `arg`: interface{} - 附加参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 解析 ZIP 文件中的 K 线数据
- 支持多种数据格式
- 处理时间戳和价格数据
- 用于历史数据导入

### LoadCalendars
加载日历数据。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 初始化基础组件
- 读取 CSV 格式的日历数据
- 按交易所分组保存日历信息
- 用于管理交易日历

### ExportKlines
导出K线数据。

参数：
- `args`: *config.CmdArgs - 命令行参数
- `prg`: utils.PrgCB - 进度回调函数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 导出指定交易对的 K 线数据
- 支持多个时间周期
- 支持调整因子处理
- 用于数据分析和备份

### PurgeKlines
清理K线数据。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 删除指定条件的 K 线数据
- 支持按交易对、时间周期筛选
- 需要用户确认后执行
- 用于数据清理和维护

### ExportAdjFactors
导出调整因子。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 导出价格调整因子数据
- 包含开始时间、因子值等信息
- 支持时区设置
- 用于价格校准和回测

### CalcCorrelation
计算相关性。

参数：
- `args`: *config.CmdArgs - 命令行参数

返回：
- `*errs.Error` - 错误信息

实现细节：
- 计算交易对之间的相关性
- 支持批量计算
- 可输出 CSV 或图像格式
- 用于交易对选择和风险控制

### RunHistKline
运行历史K线数据。

参数：
- `args`: *RunHistArgs - 运行参数

返回：
- `*errs.Error` - 错误信息

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
