# Go API 参考

编写策略先读 [strat](strat.md)；扩展数据读 [orm](orm.md) 和 [data](data.md)；截面组合与配对策略读 [factor](factor.md)。一般用户通过 YAML 和命令行使用系统，无需自行组装 Runtime。

## 按用途查找

| 包 | 用途 |
| --- | --- |
| [strat](strat.md) | 策略、StratJob、数据回调与开平仓意图 |
| [orm](orm.md) | DataSeries、任意时序字段、存储与订单模型 |
| [data](data.md) | 数据源、订阅、历史回放与实时接入 |
| [factor](factor.md) | 因子图、表达式、组合、研究与运行器 |
| [biz](biz.md) | 订单管理器、钱包和交易事件处理 |
| [goods](goods.md) | 品种筛选过滤器与刷新 |
| [exg](exg.md) | banexg 统一交易所接口与实例构造 |
| [btime](btime.md) | 模拟/实盘时钟与时间转换 |
| [config](config.md) | YAML、RunSpec、配置快照与路径解析 |
| [runtime](runtime.md) | Process/Runtime 生命周期与依赖注入 |
| [core](core.md) | 运行状态、常量和超参数类型 |
| [com](com.md) | 市场价格、进度与调度器 |
| [opt](opt.md) | 回测、优化、滚动回放与报告 |
| [live](live.md) | 实盘生命周期和远程控制边界 |
| [rpc](rpc.md) | 任务通知会话、渠道与消息模板 |
| [web](web.md) | 开发服务、实时监控与认证 |
| [entry](entry.md) | 命令扩展、引擎分派和资源装配 |
| [utils](utils.md) | 文件、集合、周期与指标工具 |

## 嵌入与兼容接口

每次回测、实盘或优化任务有自己的 `runtime.Runtime`，持有配置快照、时钟、市场/品种、策略、订单、钱包、订阅及通知。嵌入多个任务时，通过构造参数、receiver 或 `BizDeps()`/`DataDeps()` 投影传入依赖；`context.Context` 用于取消和 deadline。

文档中仍列出的包级配置、时间、订单与策略访问函数属于兼容入口，不适合在新多任务代码中查找“当前任务”。策略回调优先使用 `StratJob` 及其策略绑定的依赖。外部存储、交易所或共享账户的释放必须遵循创建方的生命周期。

`execution` 负责共享账户、逻辑持仓归属、预算、净额执行与恢复。用户侧配置和操作步骤见[实时交易](../guide/live_trading.md)与[因子指南](../guide/factor.md)；不要通过直接调用交易所写接口绕过账户执行流程。

接口签名以策略项目引用的 Banbot 版本为准。升级后重新编译，遇到不匹配时先核对依赖版本，再检查相应 API 页面。
