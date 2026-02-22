# BanBot 后端项目概述

## 项目整体概述
BanBot 是一个用于数字货币量化交易的机器人后端服务。它使用Go语言构建，提供了强大的数据处理、策略执行、订单管理和实盘交易功能。项目采用模块化设计，将核心逻辑、数据处理、交易策略、数据库交互等功能清晰分离，易于扩展和维护。

## 技术架构与实现方案
- 核心框架: 自定义事件驱动框架，支持回测与实盘模式。
- 数据库: SQLite（`banpub.db`）用于公共元数据（K线索引、日历、复权因子、范围管理、未完成K线等）；QuestDB（PGWire）可选用于大规模时序K线存储；交易/任务数据使用SQLite文件（`orders_ban.db`）。
- 数据处理: 包含数据爬虫、清洗、存储及gRPC服务，为策略提供数据支持。
- 订单管理: 支持本地模拟和实盘交易两种模式，精细化管理订单生命周期。
- 策略系统: 支持多种交易策略，通过配置文件动态加载和管理；支持运行时动态增删交易对。
- 配置管理: 使用YAML文件进行灵活配置，支持LLM模型配置继承和环境变量替换。
- 日志: 使用Zap进行高性能结构化日志记录。
- RPC: 支持通过gRPC和Webhook进行外部通信和通知。

## 核心文件索引

### 文件命名约定
- main.go: 模块/服务入口（无特殊说明时）
- common.go: 模块内通用逻辑或共享结构
- tools.go: 模块辅助工具函数

### `.` (项目根目录)
- main.go: 应用程序主入口，解析命令行参数并启动相应服务。

### `_testcom/` (测试公共组件)
- all.go: 提供测试用的公共数据和函数，如模拟的K线数据。

### `biz/` (核心业务逻辑)
- biz.go: 核心业务启动器，负责初始化服务、加载配置、刷新交易对、订单订阅等。
- data_server.go: gRPC数据服务器，对外提供特征数据流服务。
- odmgr.go: 订单管理器接口`IOrderMgr`定义，包含`ProcessOrders`、`EnterOrder`、`ExitOpenOrders`等。
- odmgr_local.go: 本地/回测模式订单管理器实现。
- odmgr_live.go: 实盘订单管理器`LiveOrderMgr`，支持同步交易所订单、监听交易、消费订单队列等。
- odmgr_live_exgs.go: 实盘订单管理器针对特定交易所的扩展逻辑。
- odmgr_local_live.go: 本地模拟实盘的订单管理器。
- telegram_order_manager.go: Telegram Bot订单管理接口，提供订单操作功能。
- trader.go: 交易管理器`Trader`，负责`OnEnvJobs`、`FeedKline`、`ExecOrders`等核心交易逻辑。
- wallet.go: 钱包管理器`BanWallets`/`ItemWallet`，维护余额、冻结、挂单、未实现盈亏等状态；支持快照保存与压缩清理。
- tools.go: K线数据导入导出、日历加载、相关性计算等工具函数。
- aifea.pb.go/aifea_grpc.pb.go: Protobuf生成的gRPC消息和服务存根。
- zh-CN/,en-US/: 嵌入式多语言资源文件。

### `btime/` (时间处理)
- main.go: 提供统一时间获取，兼容回测和实盘模式。函数：`TimeMS`、`UTCTime`、`Time`、`Now`、`ParseTimeMS`、`ToDateStr`等。
- common.go: 重试等待的通用结构和逻辑。

### `com/` (公共组件)
- common.go: 公共数据和函数。
- price.go: 全局价格管理,支持bar价格和订单簿(bid/ask)价格,提供带过期检查的安全价格获取,用于回测和实盘。
- price_live.go: 实盘价格管理,从交易所获取最新BookTicker并更新价格缓存,内置节流机制防止频繁请求。
- cron.go: 定时任务管理,基于cron库实现。

### `config/` (配置管理)
- biz.go: 配置加载解析，支持YAML合并、命令行覆盖、LLM模型配置、敏感信息脱敏。函数：`LoadConfig`、`GetConfig`、`ApplyConfig`等。
- types.go: 配置结构体定义：`Config`、`CmdArgs`、`RunPolicyConfig`、`StratPerfConfig`、`DatabaseConfig`、`APIServerConfig`等。
- cmd_types.go: 命令行参数结构体。
- cmd_biz.go: 命令行参数业务逻辑处理。

### `core/` (核心类型与全局变量)
- core.go: 设置运行模式和环境，提供`Setup`、`SetRunMode`、`SetRunEnv`、`Sleep`等函数。
- common.go: 缓存管理(ristretto)、退出回调、键生成等。函数：`GetCacheVal`、`SnapMem`、`RunExitCalls`等。
- types.go: 核心数据结构：`Param`(参数配置)、`PerfSta`(性能统计)、`JobPerf`(任务性能)等。
- data.go: 全局变量和状态，包括运行模式、市场信息、交易对管理、订单常量等，使用`deadlock.RWMutex`保护。
- calc.go: 基础计算工具，如EMA。
- errors.go: 自定义错误码和错误名称。
- utils.go: 核心层工具函数，如`GetPrice`、`SetPrice`、`IsMaker`、`SplitSymbol`等。

### `data/` (数据获取与处理)
- provider.go: 数据提供者`IProvider`接口，`HistProvider`(回测)和`LiveProvider`(实盘)实现，支持订阅预热交易对、主循环等。
- feeder.go: K线馈送器`IKlineFeeder`接口，`DBKlineFeeder`(回测)和`KlineFeeder`(实盘)实现，每个对应一个交易对的多周期数据。
- spider.go: 实盘数据爬虫`LiveSpider`，通过WebSocket/API获取实时数据，包含多个`Miner`(每个交易所+市场一个)。
- watcher.go: K线监视器`KLineWatcher`，客户端与爬虫通信，订阅K线、交易、订单簿数据。
- ws_feeder.go: WebSocket数据喂食器。
- ws_loader.go: WebSocket数据加载器。
- common.go: 数据处理通用函数和结构。
- tools.go: 数据工具，如`ReadZipCSVs`、`RunFormatTick`、`Build1mWithTicks`等。

### `entry/` (程序入口)
- main.go: 程序主入口。
- entry.go: 定义所有命令行子命令的入口函数。
- common.go: 命令分组和注册逻辑。

### `exg/` (交易所接口)
- biz.go: 交易所接口初始化和封装。函数：`Setup`、`GetWith`、`PrecCost`、`PrecPrice`、`PrecAmount`、`GetLeverage`、`GetOdBook`等。
- data.go: 交易所相关全局变量。
- ext.go: 交易所接口扩展，增加订单创建后的回调钩子。

### `goods/` (交易对筛选)
- biz.go: 交易对列表生成和筛选。函数：`Setup`、`GetPairFilters`、`RefreshPairList`。
- filters.go: 多种筛选器实现：`VolumePairFilter`、`PriceFilter`、`RateOfChangeFilter`、`SpreadFilter`、`VolatilityFilter`、`AgeFilter`等。
- types.go: 筛选器接口`IFilter`/`IProducer`和配置结构。

### `live/` (实盘交易)
- crypto_trader.go: 加密货币交易器`CryptoTrader`，初始化数据提供者、订单管理器、交易对更新钩子、任务管理、API服务等。
- common.go: 定时任务：`CronRefreshPairs`、`CronLoadMarkets`、`CronFatalLossCheck`、`CronKlineDelays`、`CronCheckTriggerOds`等。
- account_check.go: 实盘账户检查，验证交易/提现权限、IP设置、持仓模式、保证金模式，汇总余额。
- tools.go: `trade_close`命令实现，支持按账户/交易对/策略筛选平仓。

### `llm/` (LLM集成)
- config.go: LLM模型配置管理，支持模型继承、环境变量替换、定价计算。
- glm.go: GLM聊天模型实现，兼容Z.AI/智谱API，支持流式和非流式调用。
- manager.go: LLM管理器，多模型故障转移、并发控制、统计追踪和自动禁用机制。

### `opt/` (回测与优化)
- backtest.go: 回测引擎`BackTest`核心实现，包含`BTResult`结果结构。函数：`NewBackTest`、`RunBTOverOpt`、`RunRollBTPicker`等。
- hyper_opt.go: 超参数优化，支持bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes等算法。函数：`RunOptimize`、`CollectOptLog`等。
- reports.go: 生成回测报告和图表，包含性能指标计算(夏普比率、索提诺比率、最大回撤等)。
- sim_bt.go: 从日志运行滚动模拟回测。
- common.go: 回测和优化通用函数，如`AvgGoodDesc`、`DescGroups`、`DumpLineGraph`等。
- tools.go: 工具函数，如`CompareExgBTOrders`等。

### `orm/` (数据库交互)
- base.go: 数据库连接池初始化，管理QuestDB(PGWire)和SQLite(`banpub.db`/`orders_ban.db`)连接池。函数：`Setup`、`Conn`、`DbLite`等。
- db.go: sqlc生成的DBTX/Queries基础封装(QuestDB PGWire接口)。
- banpub.go: `banpub.db`(SQLite)连接和事务管理，存储公共元数据。
- pub_queries.go: `PubQueries`入口，提供banpub.db查询接口。
- pub_kline_queries.go: K线索引(`ins_kline`)和未完成K线(`kline_un`)查询操作。
- pub_meta_queries.go: 日历(`calendars`)、复权因子(`adj_factors`)等元数据批量写入。
- questdb.go: QuestDB自动安装、启动和管理，支持Linux/macOS/Windows。
- kdata.go: K线数据API下载和数据库操作封装。
- kline.go: K线数据聚合、下载、修正等高级操作，定义多周期聚合规则。
- exsymbol.go: 交易对信息数据库操作。函数：`GetExSymbols`、`GetExSymbol`、`EnsureExgSymbols`、`MapExSymbols`等。
- dump.go: 实盘时转储关键数据到文件，供调试和分析。
- qdb_queries.go: QuestDB手写查询(symbols/calendars/adj/sranges等)。
- kdata.pb.go: K线数据块Protobuf定义。
- srange.go/srange_query.go: `sranges`范围管理(支持不连续)。
- models.go: 表结构体：`ExSymbol`、`AdjFactor`、`Calendar`、`InsKline`、`KlineUn`、`SRange`等。
- tools.go: 导入导出K线、时序数据、关系数据等工具函数。
- types.go: ORM层自定义数据结构：`InfoKline`、`AdjInfo`、`KlineAgg`等。

### `orm/ormo/` (订单数据库)
- base.go: 订单数据库基础操作，任务管理、开放订单获取(带同步间隔控制)、多账户支持。
- order.go: `InOutOrder`(出入场订单)核心逻辑，状态管理、利润计算、Info字段懒加载等。
- bot_task.go: 交易任务(回测/实盘)数据库管理。
- wallet_snapshot.go: 钱包快照保存与查询，支持按小时压缩历史快照，7天后自动清理。
- data.go: 订单相关常量和枚举。
- db.go: sqlc生成的订单数据库查询接口。
- models.go: sqlc生成的订单数据库表结构体。
- trade_query.sql.go: sqlc根据订单SQL查询生成的Go代码。
- types.go: 订单相关自定义类型。

### `orm/ormu/` (UI数据库)
- base.go: UI数据库基础操作。
- db.go: sqlc生成的UI数据库查询接口。
- models.go: sqlc生成的UI数据库表结构体。
- query.go: UI任务复杂查询逻辑。
- ui_query.sql.go: sqlc根据UI SQL查询生成的Go代码。
- common.go: UI数据库通用函数。

### `rpc/` (远程过程调用)
- notify.go: 统一通知发送入口。函数：`InitRPC`、`SendMsg`、`CleanUp`。
- webhook.go: 通用Webhook实现基类`WebHook`，接口`IWebHook`定义。
- telegram.go: Telegram Bot通知实现，支持交互式命令(查询订单、余额、关闭订单等)。
- wework.go: 企业微信机器人`WeWork`通知实现。
- email.go: 邮件通知实现。
- exc_notify.go: Zap日志钩子`ExcNotify`，将错误日志通过RPC发送通知。函数：`NewExcNotify`、`TrySendExc`。

### `strat/` (策略)
- base.go: 策略基类`TradeStrat`和任务`StratJob`核心逻辑，包括仓位计算、时间周期选择、订单bar上限等。
- main.go: 策略加载和管理。
- common.go: 策略通用函数：`New`、`Get`、`GetJobs`、`CalcJobScores`、`AddOdSub`、`FireOdChange`、`LoadStratJobs`等。
- data.go: 策略全局数据结构，包括策略版本、bar环境、账户任务、WebSocket订阅任务等。
- pair_update.go: 运行时动态交易对管理`PairUpdateManager`，支持按策略增删交易对，处理关联订单平仓、WebSocket订阅注册/注销。
- goods.go: 策略相关交易对处理逻辑，包括策略分组接力和交易对评分。
- types.go: 策略自定义类型：`EnterReq`、`ExitReq`、`PairSub`、`BatchMap`、`JobEnv`等。

### `utils/` (通用工具)
- banio.go: 自定义IO操作，封装socket通信和消息协议。函数：`NewBanServer`、`NewClientIO`、`GetServerData`、`SetServerData`、`GetNetLock`等。
- biz_utils.go: 业务工具函数，如进度条`NewPrgBar`、`NewStagedPrg`等。
- correlation.go: 相关性计算和可视化。函数：`CalcCorrMat`、`GenCorrImg`、`CalcEnvsCorr`等。
- email.go: 邮件发送工具。
- file_util.go: 文件和目录操作。函数：`CopyDir`、`Copy`、`Exists`、`EnsureDir`、`FindSubPath`、`ReadCSV`、`ReadXlsx`、`WriteCsvFile`等。
- index.go: 工具包入口。
- math.go: 数学计算。函数：`DecPow`、`DecArithMean`、`DecStdDev`等。
- metrics.go: 性能指标计算。函数：`SharpeRatio`、`SortinoRatio`、`CalcExpectancy`、`CalcMaxDrawDown`、`AutoCorrPenalty`、`KMeansVals`等。
- misc.go: 杂项工具。函数：`MD5`、`IsDocker`、`OpenBrowser`、`ReadInput`、`ReadConfirm`、`ParallelRun`等。
- net_utils.go: 网络工具。函数：`DoHttp`等。
- num_utils.go: 数字处理工具。
- text_utils.go: 文本处理。函数：`SnakeToCamel`、`PadCenter`、`RandomStr`、`FormatWithMap`、`SplitSolid`、`GroupByPairQuotes`等。
- tf_utils.go: 时间周期处理工具。
- yaml_merge.go: YAML文件合并工具。

### `web/` (Web服务)
- main.go: Web服务主入口。
- base/: Web服务基础API和公共组件。
  - api_com.go: API公共函数，参数验证和错误处理。
  - api_kline.go: K线数据接口。
  - api_csv.go: CSV数据上传、列表查询和数据读取接口。
  - api_ws.go: WebSocket路由注册。
  - websocket.go: WebSocket客户端管理，支持OHLCV数据订阅。
  - indicators.go: 技术指标计算。
  - biz_com.go: 业务公共逻辑。
- dev/: 开发模式Web服务，提供策略开发、回测管理等功能。函数：`RunDev`。
  - main.go: 开发Web服务入口，初始化配置、交易所、路由注册和静态文件服务。
  - api_dev.go: 开发相关API接口。
  - strat.go: 策略管理接口。
  - data_tools.go: 数据工具接口。
  - websocket.go: 开发模式WebSocket支持。
- live/: 实盘模式Web服务，监控和管理实盘机器人。函数：`StartApi`。
  - main.go: 实盘Web服务入口。
  - auth.go: JWT认证中间件`AuthMiddleware`，支持用户登录和策略回调接口。
  - biz.go: 实盘业务API，包括余额、订单、统计、配置等。
- ui/: 嵌入式前端UI资源。

## 用户文档索引 (bandoc)

支持zh-CN/en-US两种语言版本，下面以zh-CN为例。

### `bandoc/zh-CN/` (中文文档)
- index.md: 文档首页，介绍BanBot高性能、易用性、灵活性、LLM集成、超参数调优等核心特性。

### `bandoc/zh-CN/guide/` (使用指南)
- start.md: 项目介绍、主要特征、WebUI/Dashboard展示、支持交易所、技能要求、安装要求、常见问题、社区支持。
- basic.md: 基本概念(策略、订单、品种、时间周期、指标等)、品种命名规范、主要组件(爬虫、数据提供器、订单管理器等)、策略任务约定、回测与实盘执行逻辑。
- init_project.md: 初始化新策略项目、创建策略文件、注册策略、编写入口文件。
- quick_local.md: Docker一键启动、本地安装golang、获取示例项目并编译、配置环境变量、修改配置文件、启动WebUI。
- configuration.md: 数据目录、Yaml配置文件、默认配置文件、完整配置选项(杠杆、市场类型、订单类型、单笔金额、止损、策略配置、品种过滤器等)。
- bot_usage.md: 可执行文件编译、通用命令参数、启动WebUI、启动爬虫进程、启动实时交易、回测命令、超参数优化、滚动回测等子命令用法。
- backtest.md: 回测特点、从WebUI/命令行回测、订单撮合周期(`refine_tf`)配置、超参数优化、滚动回测。
- hyperopt.md: 超参数优化命令、定义超参数范围、采样优化器、挑选器(`picker`)、过拟合陷阱、测试挑选器。
- live_trading.md: 编译(从WebUI/终端)、编辑配置、启动爬虫和机器人、消息通知、DashBoard、监控。
- pair_filters.md: 品种过滤器运行时机、所有支持的过滤器(`VolumePairList`、`PriceFilter`、`RateOfChangeFilter`、`SpreadFilter`等)及配置。
- roll_btopt.md: 滚动优化回测原理、避免过拟合、命令用法、挑选器测试、结果分析。
- strat_custom.md: 策略命名、`RunPolicyConfig`参数、`TradeStrat`对象、简单策略示例、技术指标、自定义指标、K线预热、订阅其他周期、入场离场、批量任务、止盈止损、高频数据订阅、HTTP回调。
- faq.md: 升级banbot、预编译包、支持策略类型、支持市场、稳定性、开空头、订单数量、部分退出、配置修改、常见错误等。

### `bandoc/zh-CN/advanced/` (高级功能)
- ai.md: 结合AI/机器学习的gRPC工作流、golang端特征定义、python端训练部署、LLM大模型接口集成。
- custom_cmd.md: 注册自定义终端命令、内置工具命令(K线数据一致性检查、回测报告生成、K线图生成)。
- kline_tools.md: K线存储核心(QuestDB、时间周期、`SRanges`/`KLineUn`表)、下载K线、导出导入K线(protobuf/csv.zip)、删除K线、纠正K线错误。
