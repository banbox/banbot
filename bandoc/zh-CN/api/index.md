* GO中一个包含go源码的文件夹即包(package)，是组织代码的最小单元；
* 包之间可以互相导入，但不能形成依赖环；
* 包的依赖关系和文件夹包含无关，所有包在依赖上都是平等的；（层级很深的文件夹对应包甚至可以作为项目入口包）
* 包的导入`import`语句必须放在文件顶部，不能在函数中导入，也不能动态导入;
* 整个go项目的所有包依赖关系应形成一个单向无环树。

::: tip DeepWiki
强烈推荐您通过[deepwiki](https://deepwiki.com/banbox/banbot)快速熟悉并了解banbot；其中提供了完善的流程图、设计理念、架构、对话式研究等。
:::

## Go包介绍&依赖关系
banbot 按功能特性和依赖关系划分为多个包。任务执行时，`entry` 会为每次回测、实盘或优化创建显式的 `runtime.Runtime`；配置、时钟、市场与交易对、策略、订单、钱包和数据依赖随该 Runtime 一起传递，而不是以包级可变状态作为任务之间的通信方式。下面是各包的依赖关系：
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
* orm包含 QuestDB/TimescaleDB 的时序数据库读写，包括 OHLCV、任意自定义序列、品种、覆盖范围、交易日历和复权因子；
* ormo包含订单相关：交易任务BotTask，持仓记录InOutOrder、交易所订单ExOrder；
* ormu包含WebUI相关：回测任务记录。

&emsp;exg config  
#### [data](data.md)
回测和实盘的数据读取、预热、订阅等。
* IProvider 是交易所市场下多品种 OHLCV 数据提供者，IKlineFeeder 是单品种 K 线数据提供者。一个 IProvider 可包含多个 IKlineFeeder，一个 IKlineFeeder 可包含多个周期数据。
* IProvider的HistProvider对应回测数据提供者，LiveProvider对应实盘数据提供者（会从spider进程订阅数据）。
* IKlineFeeder的DBKlineFeeder对应回测，KlineFeeder对应实盘；TfKlineLoader可用于分批加载某个品种的指定周期K线，然后逐个读取的场景；一个KlineFeeder可包含多个TfKlineLoader。
* Spider是公共数据实时订阅爬虫进程。可同时订阅多个交易所、多个市场、多个品种的价格、订单簿、K线等数据。一个Spider可供多个实盘机器人进程连接访问。
* KLineWatcher用于接收来自Spider数据的客户端。被LiveProvider使用。
* DataSource、SeriesRuntime 和 HistSeriesFeeder 用于任意自定义时序数据的注册、历史回填、实时订阅与回放。

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


## 任务运行态与兼容边界

`runtime.Process` 只管理少量进程级构造资源，例如 Runtime ID 与按存储身份共享的 SID 分配器。每个 `runtime.Runtime` 则拥有该任务的 `core.State`、`config.Snapshot`、`btime.ClockState`、市场和交易对状态、策略任务、订单与钱包、调度器、通知、数据目录和显式存储/交易所依赖。业务包通过 `Runtime.BizDeps()` 或 `Runtime.DataDeps()` 接收所需的窄依赖。

因此，普通入口创建的回测、实盘和优化任务具有各自的取消、关闭和可变业务状态；同一 `Process` 可以创建多个 Runtime。`context.Context` 仅用于取消、deadline 与 I/O 生命周期，不承载业务状态。

为兼容旧的嵌入式调用和部分维护命令，仓库仍保留少量包级 facade（例如旧的配置、时间与交易状态访问接口）。这些路径由兼容边界保护，不能作为新任务的状态访问方式；新代码应从构造参数、具体 receiver 或 Runtime 依赖投影取得状态。
