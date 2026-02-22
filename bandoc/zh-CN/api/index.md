* GO中一个包含go源码的文件夹即包(package)，是组织代码的最小单元；
* 包之间可以互相导入，但不能形成依赖环；
* 包的依赖关系和文件夹包含无关，所有包在依赖上都是平等的；（层级很深的文件夹对应包甚至可以作为项目入口包）
* 包的导入`import`语句必须放在文件顶部，不能在函数中导入，也不能动态导入;
* 整个go项目的所有包依赖关系应形成一个单向无环树。

::: tip DeepWiki
强烈推荐您通过[deepwiki](https://deepwiki.com/banbox/banbot)快速熟悉并了解banbot；其中提供了完善的流程图、设计理念、架构、对话式研究等。
:::

## Go包介绍&依赖关系
banbot按功能特性和依赖关系划分了若干不同的包，而常用的一些全局变量大多分散在多个包，下面是全部包的依赖关系：
#### [core](core.md)
被所有其他包引用的一些类型、方法、常量和变量；如超参数定义、价格、简单Ema、错误代码等。
#### [btime](btime.md)
时间工具包，获取当前模拟时间、获取UTC时间、时间转换

&emsp;core
#### [utils](utils.md)
工具包，BanIO Tcp进程间通信、进度条、相关性计算、聚类、文件读写、夏普等计算、其他工具函数

&emsp;core btime
#### [config](config.md)
解析yml的配置，若干yml访问变量

&emsp;core btime utils
#### [exg](exg.md)
交易所对象访问管理

&emsp;config utils core
#### [orm](orm.md)
orm被其下的ormo,ormu两个子包引用。
* orm包含时序数据库读写访问如K线kline、品种exsymbol、空洞kHole、范围kInfo、交易日历、复权因子；
* ormo包含订单相关：交易任务BotTask，持仓记录InOutOrder、交易所订单ExOrder；
* ormu包含WebUI相关：回测任务记录。

&emsp;exg config  
#### [data](data.md)
回测和实盘K线数据读取、预热、订阅等。
* IProvider是某交易所某市场下多品种K线数据提供者，IKlineFeeder是单品种K线数据提供者。一个IProvider可包含多个IKlineFeeder，一个IKlineFeeder可包含多个周期数据。
* IProvider的HistProvider对应回测数据提供者，LiveProvider对应实盘数据提供者（会从spider进程订阅数据）。
* IKlineFeeder的DBKlineFeeder对应回测，KlineFeeder对应实盘；TfKlineLoader可用于分批加载某个品种的指定周期K线，然后逐个读取的场景；一个KlineFeeder可包含多个TfKlineLoader。
* Spider是公共数据实时订阅爬虫进程。可同时订阅多个交易所、多个市场、多个品种的价格、订单簿、K线等数据。一个Spider可供多个实盘机器人进程连接访问。
* KLineWatcher用于接收来自Spider数据的客户端。被LiveProvider使用。

&emsp;orm exg config 
#### [strat](strat.md)
交易策略、交易任务管理、交易任务初始化。
* TradeStrat是经典时序策略结构体，StratJob是TradeStrat在某个品种的交易任务
* 在刷新交易品种后，可通过LoadStratJobs初始化策略任务

&emsp;orm utils
#### [goods](goods.md)
品种过滤器，对应yml中的pairlists；当`pairs`为空时才会被使用。可使用预设的过滤器，对全部可交易品种、按交易量排序、按价格、波动率、相关性、上市时间、偏移量等进行过滤。

&emsp;orm exg 
#### rpc
社交app消息通知

&emsp;btime, core, config, utils
#### [biz](biz.md)
重要业务逻辑包。包含回测/实盘订单管理器、钱包、Grpc数据Server端、基础Trader处理K线更新技术指标和StratJob相关回调；
另包含K线导入导出、相关性计算、交易所订单下载等工具函数

&emsp;exg orm strat goods data rpc
#### [opt](opt.md)
包含回测、超参数优化等。BackTestLite用于简单回测、BackTest用于复杂逻辑回测，可包含BackTestLite用于未完成订单接力入场。
* hyper_opt中使用贝叶斯、cmaes等6中优化器搜索策略超参数
* sim_bt滚动模拟回测，使用超参数优化输出的日志结果。最终得到无未来函数真实的回测报告。

&emsp;biz data orm goods strat
### web
WebUI和Dashboard UI的服务器端&前端资源。

&emsp;config core utils orm data orm exg btime strat opt biz
#### [live](live.md)
实时交易（实盘/模拟运行），启动相关cron定时任务，监听交易所订单变化等

&emsp;biz data orm goods strat opt rpc
#### [entry](entry.md)
所有cmd子命令注册和业务逻辑入口

&emsp;optmize live data 


## 各个Go包中的重要全局变量
```text
core
    Ctx context.Context // 全局上下文，可select <- core.Ctx响应全局退出事件
    StopAll func() // 发出全局退出事件
    NoEnterUntil map[string]int64 // 在给定截止时间戳之前禁止开单
    RunMode string // RunModeLive / RunModeBackTest / RunModeOther
    RunEnv string // RunEnvProd / RunEnvTest / RunEnvDryRun
    StartAt int64 // 启动时间，13位时间戳
    LiveMode bool // 是否是实时模式：实盘+模拟运行
    BackTestMode bool // 是否是回测模式
    EnvReal bool // LiveMode && RunEnv != RunEnvDryRun 提交订单到交易所run_env:prod/test
    ExgName string // 当前交易所名称
    Market string // 当前市场
    IsContract bool // 当前市场是否是合约市场, linear/inverse/option
    Pairs []string // 全局所有的标的，按标的刷新后的顺序
    OdBooks map[string]*banexg.OrderBook // 缓存所有收到的订单簿
    
btime
    CurTimeMS int64 // 当前时间戳，仅回测模式下使用
    LocShow *time.Location // 用于显示的时区
    UTCLocale *time.Location

biz
    AccOdMgrs // 订单簿对象，必定不为空
    AccLiveOdMgrs // 实盘订单簿对象，实盘时不为空
    AccWallets // 钱包
    
data
    Spider // 爬虫

orm
    HistODs // 已平仓的订单：全部出场，仅回测使用
    AccOpenODs // 打开的订单：尚未提交、已提交未入场、部分入场，全部入场，部分出场
    AccTriggerODs // 尚未提交的限价入场单，等待轮询提交到交易所，仅实盘使用
    AccTaskIDs // 当前任务ID
    AccTasks // 当前任务

exg
    Default  // 交易所对象

strat
    StagyMap // 策略注册map
    Versions // 策略版本
    Envs // 涉及的所有K线环境：Pair+TimeFrame
    PairTFStags // 标的+TF+策略 pair:[stratID]TradeStrat
    AccJobs // 涉及的所有标的 account: pair_tf: [stratID]StratJob
    AccInfoJobs // 涉及的所有辅助信息标的 account: pair_tf: [stratID]StratJob
    ForbidJobs // 禁止创建的策略任务 pair_TF: stratID: empty
```
注意：所有Acc开头的变量都是支持多账户的map
