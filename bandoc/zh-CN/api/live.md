# live 包

live 包提供了实时交易相关的功能。

## 定时任务相关方法

### CronRefreshPairs
定时刷新交易对信息的定时任务。

参数：
- `dp data.IProvider` - 数据提供者接口

功能：
- 根据配置的Cron表达式定期刷新交易对信息
- 通过`biz.AutoRefreshPairs`执行实际的刷新操作

### CronLoadMarkets
定时加载市场行情数据的定时任务。

功能：
- 每2小时更新一次市场行情数据
- 通过`orm.LoadMarkets`从交易所获取最新市场数据

### CronFatalLossCheck
定时检查致命性损失的定时任务。

功能：
- 定期检查交易账户是否出现致命性损失
- 根据配置的`FatalStop`阈值进行检查

### CronKlineDelays
定时检查K线延迟情况的定时任务。

功能：
- 每30秒执行一次检查
- 监控K线数据的延迟情况，确保数据及时性

### CronKlineSummary
定时生成K线数据摘要的定时任务。

功能：
- 每5分钟执行一次
- 生成K线数据的统计摘要信息

### CronCheckTriggerOds
定时检查触发订单的定时任务。

功能：
- 每分钟检查限价订单是否触发提交
- 监控市场条件是否满足订单触发条件

## 交易相关类和方法

### NewCryptoTrader
创建一个新的加密货币交易器实例。

返回：
- `*CryptoTrader` - 新创建的交易器实例

## Web API相关方法

### StartApi
启动API服务器。

返回：
- `*errs.Error` - 如果启动失败返回错误信息

### AuthMiddleware
创建一个用于API认证的中间件。

参数：
- `secret string` - 用于验证的密钥

返回：
- `fiber.Handler` - Fiber框架的处理器函数

## 结构体定义

### CryptoTrader
加密货币交易器，负责管理和执行加密货币的交易操作。

**字段:**
- `Trader` (biz.Trader): 基础交易器，包含基本的交易功能
- `dp` (*data.LiveProvider): 实时数据提供者，用于获取实时市场数据

### OrderArgs
订单查询参数结构体，用于过滤和获取订单信息。

**字段:**
- `StartMs` (int64): 开始时间戳(毫秒)
- `StopMs` (int64): 结束时间戳(毫秒)
- `Limit` (int): 返回结果数量限制
- `AfterID` (int): 查询指定ID之后的订单
- `Symbols` (string): 交易对符号，多个用逗号分隔
- `Status` (string): 订单状态(open/his)
- `Strategy` (string): 策略名称
- `TimeFrame` (string): 时间周期
- `Source` (string): 数据来源，必填(bot/exchange/position)

### OdWrap
订单包装结构体，扩展了基础订单信息。

**字段:**
- `InOutOrder` (*ormo.InOutOrder): 基础订单信息
- `CurPrice` (float64): 当前价格

### ForceExitArgs
强制平仓参数结构体。

**字段:**
- `OrderID` (string): 订单ID，必填。使用"all"表示全部订单

### CloseArgs
平仓参数结构体。

**字段:**
- `Symbol` (string): 交易对符号，必填。使用"all"表示全部持仓
- `Side` (string): 方向(long/short)
- `Amount` (float64): 数量
- `OrderType` (string): 订单类型
- `Price` (float64): 价格

### DelayArgs
延迟进场参数结构体。

**字段:**
- `Secs` (int64): 延迟秒数，必填

### JobItem
策略任务项结构体。

**字段:**
- `Pair` (string): 交易对
- `Strategy` (string): 策略名称
- `TF` (string): 时间周期
- `Price` (float64): 当前价格
- `OdNum` (int): 当前订单数量

### PairArgs
交易对查询参数结构体。

**字段:**
- `Start` (int64): 开始时间戳
- `Stop` (int64): 结束时间戳

### GroupItem
分组统计项结构体。

**字段:**
- `Key` (string): 分组键值
- `HoldHours` (float64): 持仓时长(小时)
- `TotalCost` (float64): 总成本
- `ProfitSum` (float64): 总盈亏
- `ProfitPct` (float64): 盈亏百分比
- `CloseNum` (int): 平仓次数
- `WinNum` (int): 盈利次数

### PerfArgs
绩效查询参数结构体。

**字段:**
- `GroupBy` (string): 分组方式(symbol/month/week/day)
- `Pairs` ([]string): 交易对列表
- `StartSecs` (int64): 开始时间戳(秒)
- `StopSecs` (int64): 结束时间戳(秒)
- `Limit` (int): 返回结果数量限制

### LogArgs
日志查询参数结构体。

**字段:**
- `Num` (int): 返回的日志行数

### AuthClaims
JWT认证声明结构体。

**字段:**
- `User` (string): 用户名
- `RegisteredClaims` (jwt.RegisteredClaims): JWT标准声明

### LoginRequest
登录请求参数结构体。

**字段:**
- `Username` (string): 用户名，必填
- `Password` (string): 密码，必填
