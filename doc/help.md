# BanBot 后端项目概述

## 项目整体概述
BanBot 是一个用于数字货币量化交易的机器人后端服务。它使用Go语言构建，提供了强大的数据处理、策略执行、订单管理和实盘交易功能。项目采用模块化设计，将核心逻辑、数据处理、交易策略、数据库交互等功能清晰分离，易于扩展和维护。

## 技术架构与实现方案
- 核心框架: 自定义事件驱动框架，支持回测与实盘模式。
- 数据库: PostgreSQL，通过自定义ORM层进行数据交互。
- 数据处理: 包含数据爬虫、清洗、存储及gRPC服务，为策略提供数据支持。
- 订单管理: 支持本地模拟和实盘交易两种模式，精细化管理订单生命周期。
- 策略系统: 支持多种交易策略，通过配置文件动态加载和管理。
- 配置管理: 使用YAML文件进行灵活配置。
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
- biz.go: 项目核心业务逻辑的启动器和协调器，负责初始化各项服务，嵌入默认配置文件。
- data_server.go: gRPC数据服务器，对外提供特征数据流服务。
- odmgr.go: 订单管理器接口定义和通用逻辑。
- odmgr_local.go: 本地/回测模式的订单管理器实现。
- odmgr_live.go: 实盘模式的订单管理器实现。
- odmgr_live_exgs.go: 实盘订单管理器针对特定交易所的扩展逻辑。
- odmgr_local_live.go: 本地模拟实盘的订单管理器。
- telegram_order_manager.go: Telegram Bot订单管理接口实现,为rpc/telegram.go提供订单操作功能。
- trader.go: 交易员核心逻辑，处理K线数据并驱动策略执行。
- wallet.go: 钱包管理，包括余额、冻结、挂单等状态的维护；dry-run/实盘 钱包快照保存与7天后按小时压缩清理。
- tools.go
- aifea.pb.go: Protobuf生成的gRPC消息结构体。
- aifea_grpc.pb.go: Protobuf生成的gRPC服务客户端和服务器存根。
- zh-CN/,en-US/: 嵌入式多语言资源文件。

### `btime/` (时间处理)
- main.go: 提供统一的时间获取功能,兼容回测和实盘模式。
- common.go: 定义了重试等待的通用结构和逻辑。

### `com/` (公共组件)
- common.go: 公共数据和函数。
- price.go: 全局价格管理,支持bar价格和订单簿价格,用于回测和实盘。
- cron.go: 定时任务管理,基于cron库实现。

### `config/` (配置管理)
- biz.go: 负责加载和解析项目配置文件。
- types.go: 定义了项目的所有配置项结构体。
- cmd_types.go: 定义了命令行参数相关的结构体。
- cmd_biz.go: 命令行参数的业务逻辑处理。

### `core/` (核心类型与全局变量)
- core.go: 定义项目运行模式、环境等核心全局变量和函数。
- common.go: 公共函数,包括缓存管理(ristretto)、退出回调、键生成等。
- types.go: 定义了项目中最基础、最核心的数据结构。
- data.go: 定义了与数据相关的全局变量和状态,包括运行模式、市场信息、交易对管理、禁单控制等。
- calc.go: 提供了基础的计算工具，如EMA（指数移动平均）。
- errors.go: 定义了项目自定义的错误码和错误名称。
- utils.go: 提供了核心层的工具函数。

### `data/` (数据获取与处理)
- provider.go: 数据提供者接口和实现，负责为回测和实盘提供统一的数据源。
- feeder.go: 数据喂食器，负责从数据源获取数据并推送到策略。
- spider.go: 实盘数据爬虫，通过WebSocket或API从交易所获取实时数据。
- watcher.go: K线数据监听器，用于客户端与爬虫之间的通信。
- ws_feeder.go: WebSocket数据喂食器,用于实时模式。
- ws_loader.go: WebSocket数据加载器,管理实时数据流。
- common.go: 数据处理模块的通用函数和结构。
- tools.go

### `entry/` (程序入口)
- main.go: (文件内容未提供) 可能的程序主入口。
- entry.go: 定义了所有命令行子命令的入口函数。
- common.go: 定义了命令的分组和注册逻辑。

### `exg/` (交易所接口)
- biz.go: 交易所接口的初始化和封装。
- data.go: 定义了交易所相关的全局变量。
- ext.go: 对交易所接口进行扩展，增加了订单创建后的回调钩子。

### `goods/` (交易对筛选)
- biz.go: 交易对列表的生成和筛选逻辑。
- filters.go: 实现了多种交易对筛选器，如按成交量、价格、波动率等。
- types.go: 定义了筛选器相关的配置结构。

### `live/` (实盘交易)
- crypto_trader.go: 加密货币实盘交易的主逻辑，负责启动和管理整个实盘流程。
- common.go: 实盘模式下的定时任务和公共函数。
- tools.go

### `opt/` (回测与优化)
- backtest.go: 回测引擎的核心实现。
- hyper_opt.go: 超参数优化的实现，支持多种优化算法。
- reports.go: 生成回测报告和图表的逻辑。
- sim_bt.go: 从日志运行滚动模拟回测的逻辑。
- common.go: 回测和优化模块的通用函数和结构。
- tools.go

### `orm/` (数据库交互)
- base.go: 数据库连接池的初始化和基础操作。
- db.go: sqlc生成的数据库查询接口。
- kdata.go: K线数据的数据库操作封装。
- exsymbol.go: 交易对信息的数据库操作封装。
- dump.go: 用于在实盘时转储关键数据到文件，供调试和分析。
- copyfrom.go: sqlc生成的PostgreSQL `COPY FROM` 功能，用于批量数据导入。
- pg_query.sql.go: sqlc根据SQL查询生成的Go代码。
- kdata.pb.go: K线数据块的Protobuf定义。
- kinfo.go: K线信息（如数据范围）的查询逻辑。
- kline.go: K线数据的聚合、下载、修正等高级操作。
- models.go: sqlc生成的数据库表结构体。
- tools.go
- types.go: ORM层自定义的数据结构。

### `orm/ormo/` (订单数据库)
- base.go: 订单相关数据库的基础操作和全局变量。
- order.go: `InOutOrder`（出入场订单）的核心逻辑，包括状态管理、利润计算等。
- bot_task.go: 交易任务（回测/实盘）的数据库管理。
- data.go: 定义了订单相关的常量和枚举。
- db.go: sqlc生成的订单数据库查询接口。
- models.go: sqlc生成的订单数据库表结构体。
- trade_query.sql.go: sqlc根据订单相关SQL查询生成的Go代码。
- types.go: 订单相关的自定义类型。

### `orm/ormu/` (UI数据库)
- base.go: UI相关数据库的基础操作。
- db.go: sqlc生成的UI数据库查询接口。
- models.go: sqlc生成的UI数据库表结构体。
- query.go: UI任务的复杂查询逻辑。
- ui_query.sql.go: sqlc根据UI相关SQL查询生成的Go代码。
- common.go: UI数据库的通用函数。

### `rpc/` (远程过程调用)
- notify.go: 统一的通知发送入口。
- webhook.go: 通用的Webhook实现基类。
- telegram.go: Telegram Bot通知实现,支持交互式命令(查询订单、余额、关闭订单等)。
- wework.go: 企业微信机器人的通知实现。
- email.go: 邮件通知的实现。
- exc_notify.go: Zap日志钩子,用于将错误日志通过RPC发送通知。

### `strat/` (策略)
- base.go: 策略基类和核心逻辑，定义了策略接口和`StratJob`结构。
- main.go: 策略的加载和管理。
- common.go: 策略相关的通用函数和全局变量。
- data.go: 定义了策略模块使用的全局数据结构。
- goods.go: 策略相关的交易对处理逻辑。
- types.go: 策略相关的自定义类型定义。

### `utils/` (通用工具)
- banio.go: 自定义的IO操作，封装了socket通信和消息协议。
- biz_utils.go: 业务相关的通用工具函数，如进度条。
- correlation.go: 相关性计算和可视化。
- email.go: 邮件发送工具。
- file_util.go: 文件和目录操作的工具函数。
- index.go: 工具包的入口。
- math.go: 数学计算相关的工具函数。
- metrics.go: 性能指标计算，如夏普比率、索提诺比率。
- misc.go: 其他杂项工具函数。
- net_utils.go: 网络相关的工具函数。
- num_utils.go: 数字处理相关的工具函数。
- text_utils.go: 文本处理相关的工具函数。
- tf_utils.go: 时间周期（TimeFrame）处理的工具函数。
- yaml_merge.go: YAML文件合并工具。

### `web/` (Web服务)
- main.go
- base/: 提供了Web服务的基础API和公共组件。
  - api_com.go: API公共函数,包括参数验证和错误处理。
  - api_kline.go: K线数据接口。
  - api_ws.go: WebSocket路由注册。
  - websocket.go: WebSocket客户端管理,支持OHLCV数据订阅。
  - indicators.go: 技术指标计算。
  - biz_com.go: 业务公共逻辑。
- dev/: 开发模式下的Web服务,提供了策略开发、回测管理等功能。
  - main.go
  - api_dev.go: 开发相关API接口。
  - strat.go: 策略管理接口。
  - data_tools.go: 数据工具接口。
  - websocket.go: 开发模式WebSocket支持。
- live/: 实盘模式下的Web服务,用于监控和管理实盘机器人。
  - main.go
  - auth.go: JWT认证中间件,支持用户登录和策略回调接口。
  - biz.go: 实盘业务API,包括余额、订单、统计、配置等。
- ui/: 嵌入式的前端UI资源。
