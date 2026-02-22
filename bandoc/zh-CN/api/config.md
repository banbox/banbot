# config 包

config 包提供了系统配置相关的结构体和方法。

## 重要结构体

### CmdArgs
命令行参数结构体,包含以下字段:
- `Configs`: []string - 配置文件路径列表
- `Logfile`: string - 日志文件路径
- `DataDir`: string - 数据目录路径
- `NoCompress`: bool - 是否不压缩
- `NoDefault`: bool - 是否不使用默认配置
- `LogLevel`: string - 日志级别
- `TimeRange`: string - 时间范围,格式为"YYYYMMDD-YYYYMMDD"
- `TimeFrames`: []string - 时间周期列表
- `StakeAmount`: float64 - 单笔开单金额
- `StakePct`: float64 - 单笔开单金额百分比
- `Pairs`: []string - 交易对列表
- `Force`: bool - 是否强制执行
- `WithSpider`: bool - 是否使用爬虫
- `MaxPoolSize`: int - 最大连接池大小
- `CPUProfile`: bool - 是否开启CPU性能分析
- `MemProfile`: bool - 是否开启内存性能分析
- `TimeZone`: string - 时区
- `OptRounds`: int - 超参数优化单任务执行轮次
- `Concur`: int - 超参数优化多进程并发数量
- `Sampler`: string - 超参数优化方法(tpe/bayes/random/cmaes等)
- `EachPairs`: bool - 是否逐个标的执行
- `ReviewPeriod`: string - 持续调参回测时,调参回顾的周期
- `RunPeriod`: string - 持续调参回测时,调参后有效运行周期
- `Alpha`: float64 - 计算EMA的平滑因子
- `Separate`: bool - 回测时是否策略组合单独测试

### Config
根配置结构体,包含以下字段:
- `Name`: string - 配置名称
- `Env`: string - 运行环境
- `Leverage`: float64 - 杠杆倍数
- `LimitVolSecs`: int - 限价单预期等待成交时间(秒)
- `PutLimitSecs`: int - 限价单提交到交易所的预期时间
- `MarketType`: string - 市场类型
- `ContractType`: string - 合约类型
- `OdBookTtl`: int64 - 订单簿生存时间
- `StopEnterBars`: int - 入场限价单超过多少个蜡烛未成交则取消
- `ConcurNum`: int - 并发数量
- `OrderType`: string - 订单类型
- `PreFire`: float64 - 预发射
- `MarginAddRate`: float64 - 追加保证金比率
- `ChargeOnBomb`: bool - 是否在爆仓时收费
- `TakeOverStgy`: string - 接管策略
- `StakeAmount`: float64 - 单笔开单金额
- `StakePct`: float64 - 单笔开单金额百分比
- `MaxStakeAmt`: float64 - 单笔最大开单金额
- `OpenVolRate`: float64 - 开单数量倍率
- `MinOpenRate`: float64 - 最小开单比率
- `BTNetCost`: float64 - 回测网络延迟(秒)
- `MaxOpenOrders`: int - 最大开单数量
- `MaxSimulOpen`: int - 最大同时开单数量
- `WalletAmounts`: map[string]float64 - 钱包金额
- `DrawBalanceOver`: float64 - 提取余额阈值
- `StakeCurrency`: []string - 开单币种
- `FatalStop`: map[string]float64 - 致命停止条件
- `FatalStopHours`: int - 致命停止小时数
- `TimeRange`: *TimeTuple - 时间范围
- `RunTimeframes`: []string - 运行时间周期
- `KlineSource`: string - K线数据源
- `WatchJobs`: map[string][]string - 监控任务
- `RunPolicy`: []*RunPolicyConfig - 运行策略配置
- `StratPerf`: *StratPerfConfig - 策略性能配置
- `Pairs`: []string - 交易对列表
- `PairMgr`: *PairMgrConfig - 交易对管理配置
- `PairFilters`: []*CommonPairFilter - 交易对过滤器
- `Exchange`: *ExchangeConfig - 交易所配置
- `Database`: *DatabaseConfig - 数据库配置
- `APIServer`: *APIServerConfig - API服务器配置
- `RPCChannels`: map[string]map[string]interface{} - RPC通道配置
- `Webhook`: map[string]map[string]string - Webhook配置

### RunPolicyConfig
运行策略配置,可以同时运行多个策略,包含以下字段:
- `Name`: string - 策略名称
- `Filters`: []*CommonPairFilter - 交易对过滤器
- `RunTimeframes`: []string - 运行时间周期
- `MaxPair`: int - 最大交易对数量
- `MaxOpen`: int - 最大开单数量
- `MaxSimulOpen`: int - 最大同时开单数量
- `Dirt`: string - 交易方向(long/short)
- `StratPerf`: *StratPerfConfig - 策略性能配置
- `Pairs`: []string - 交易对列表
- `Params`: map[string]float64 - 策略参数
- `PairParams`: map[string]map[string]float64 - 交易对参数

### StratPerfConfig
策略性能配置,包含以下字段:
- `Enable`: bool - 是否启用
- `MinOdNum`: int - 最小订单数量
- `MaxOdNum`: int - 最大订单数量
- `MinJobNum`: int - 最小任务数量
- `MidWeight`: float64 - 中等权重
- `BadWeight`: float64 - 差权重

### DatabaseConfig
数据库配置,包含以下字段:
- `Url`: string - 数据库连接URL
- `Retention`: string - 数据保留时间
- `MaxPoolSize`: int - 最大连接池大小
- `AutoCreate`: bool - 是否自动创建数据库

### APIServerConfig
API服务器配置,包含以下字段:
- `Enable`: bool - 是否启用
- `BindIPAddr`: string - 绑定IP地址
- `Port`: int - 监听端口
- `Verbosity`: string - 日志详细程度
- `JWTSecretKey`: string - JWT密钥
- `CORSOrigins`: []string - CORS允许的源
- `Users`: []*UserConfig - 用户配置

### UserConfig
用户配置,包含以下字段:
- `Username`: string - 用户名
- `Password`: string - 密码
- `AccRoles`: map[string]string - 账户角色权限
- `ExpireHours`: float64 - Token过期时间(小时)

### WeWorkChannel
企业微信通道配置,包含以下字段:
- `Enable`: bool - 是否启用
- `Type`: string - 类型
- `MsgTypes`: []string - 消息类型
- `AgentId`: string - 应用ID
- `CorpId`: string - 企业ID
- `CorpSecret`: string - 应用密钥
- `Keywords`: string - 关键词

### PairMgrConfig
交易对管理配置,包含以下字段:
- `Cron`: string - 定时任务表达式
- `Offset`: int - 偏移量
- `Limit`: int - 限制数量
- `ForceFilters`: bool - 是否强制过滤

### AccountConfig
账户配置,包含以下字段:
- `APIKey`: string - API密钥
- `APISecret`: string - API密钥
- `NoTrade`: bool - 是否禁止交易
- `MaxStakeAmt`: float64 - 单笔最大开单金额
- `StakeRate`: float64 - 开单金额倍数
- `StakePctAmt`: float64 - 按百分比开单时的金额
- `Leverage`: float64 - 杠杆倍数

## 公开方法

### GetDataDir
获取数据目录路径。

返回：
- `string` - 数据目录的绝对路径。如果环境变量 `BanDataDir` 未设置,返回空字符串。

### GetStratDir 
获取策略目录路径。

返回：
- `string` - 策略目录的绝对路径。如果环境变量 `BanStratDir` 未设置,返回空字符串。

### LoadConfig
加载并应用配置。

参数：
- `args`: *CmdArgs - 命令行参数对象

返回：
- `*errs.Error` - 错误信息,如果成功则返回 nil

### GetConfig
根据命令行参数获取配置。

参数：
- `args`: *CmdArgs - 命令行参数对象
- `showLog`: bool - 是否显示日志

返回：
- `*Config` - 配置对象
- `*errs.Error` - 错误信息

### ParseConfig
解析指定路径的配置文件。

参数：
- `path`: string - 配置文件路径

返回：
- `*Config` - 配置对象
- `*errs.Error` - 错误信息

### ApplyConfig
应用配置到全局状态。

参数：
- `args`: *CmdArgs - 命令行参数对象
- `c`: *Config - 配置对象

返回：
- `*errs.Error` - 错误信息

### GetExgConfig
获取当前交易所配置。

返回：
- `*ExgItemConfig` - 交易所配置对象

### GetTakeOverTF
获取指定交易对的接管时间周期。

参数：
- `pair`: string - 交易对名称
- `defTF`: string - 默认时间周期

返回：
- `string` - 时间周期

### GetAccLeverage
获取指定账户的杠杆倍数。

参数：
- `account`: string - 账户名称

返回：
- `float64` - 杠杆倍数

### ParsePath
解析路径,将 `$` 或 `@` 开头的路径替换为数据目录的绝对路径。

参数：
- `path`: string - 原始路径

返回：
- `string` - 解析后的路径

### GetStakeAmount
获取指定账户的下注金额。

参数：
- `accName`: string - 账户名称

返回：
- `float64` - 下注金额

### DumpYaml
将当前配置导出为 YAML 格式。

返回：
- `[]byte` - YAML 格式的配置数据
- `*errs.Error` - 错误信息

### LoadPerfs
从指定目录加载策略性能数据。

参数：
- `inDir`: string - 输入目录路径

### ParseTimeRange
解析时间范围字符串。

参数：
- `timeRange`: string - 时间范围字符串,格式为 "YYYYMMDD-YYYYMMDD"

返回：
- `int64` - 开始时间戳(毫秒)
- `int64` - 结束时间戳(毫秒)
- `error` - 错误信息

### GetExportConfig
获取导出配置。

参数：
- `path`: string - 配置文件路径

返回：
- `*ExportConfig` - 导出配置对象
- `*errs.Error` - 错误信息
