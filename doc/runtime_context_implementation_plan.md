# Runtime context 改造执行计划

依据 `runtime_context_review_37cade3.md` 的 R1–R11 执行 High/Medium 修复。目标是保持领域包依赖方向和 `orm.DataSeries.Values map[string]any` 契约，同时让运行依赖由具体 State/owner 持有，不使用 `context.Value` 或通用 service map。

## 完成状态

| 编号 | 优先级 | 状态 | 说明 |
| --- | --- | --- | --- |
| R1 | High | 主要修复完成/测试验收通过 | 显式 Cobra `backtest`/`trade` 已使用独立 Runtime session；旧导出入口仍保留 legacy gate。`opt`/`live` 的确定性双 runner 并发、独立取消和串行基线一致性测试已通过；真实数据库、固定行情和生产交易所的完整 runner 演练仍待环境验收。 |
| R2 | High | 已完成主要修复 | `orm.Storage` 绑定连接池、后端和协调身份，显式 symbol/series 查询不再从进程 registry 推断 SID。 |
| R3 | High | 已完成 | banexg 已发布并固定为 `github.com/banbox/banexg v0.2.64`；`go.mod` 删除本机绝对路径 replace。 |
| R4 | Medium | 已完成 | entry session 对 `Process` 负责 Close，且 gate 在资源清理完成后才释放。 |
| R5 | Medium | 已完成 | 架构文档、ownership、legacy 边界和验证限制已同步。 |
| R6 | High | 已完成 | QuestDB rewrite 在清理失败时保留 swap/recovery intent，替换前验证和 WAL 可见性等待保留。 |
| R7 | Medium | 已完成 | scheduler 在构造时绑定 location/lang，不读取全局时区/NTP 配置。 |
| R8 | Medium | 已完成 | Runtime 构造失败统一释放 Core 子 context 和已创建资源。 |
| R9 | High | 已完成 | source 收集、激活和 K 线字段投影绑定 `strat.State`；补有相同 SID/timeframe 的双 State 隔离回归。 |
| R10 | High | 已完成 | backtest 范围、strict、时钟、输出目录和 scheduler 使用实例 deps/config；兼容 API 的 legacy 分支明确保留。 |
| R11 | Medium | 已完成 | 订单历史过滤绑定 `ormo.OrderState`，实例清理不会改写 legacy 历史。 |

## 实施原则

- 领域 State 留在原包，`runtime.Runtime` 只负责组合和生命周期；固定依赖用结构体字段、具体 receiver 和窄接口传递。
- `context.Context` 只负责取消、deadline 和 I/O；不通过 `Value` 传递运行状态。
- `DataSeries.Values` 继续承载任意字段、类型、缺失值和显式 `nil`；不引入 typed OHLCV 快速路径。
- banbot 不增加交易所特有分支；交易所 capability 和语义由 banexg adapter 提供。
- QuestDB 按写后读异步模型处理：等待目标可见，硬错误立即返回，替换前验证快照，超时保留 recovery marker。
- legacy facade 只在兼容入口安装 gate；显式 Runtime 不静默回退到 package global。

## 已执行切片

1. 建立 `Process -> Runtime` typed ownership，接线 Core/Config/Clock/Market/Symbols/Storage/Strategies/Orders/Trading/Cron/Notifications/Exchange。
2. 为 ORM 增加显式 `Storage`、Queries/SeriesRepo 绑定、SID allocator/recovery 协调和 QuestDB 可见性/替换保护。
3. 将 feeder/provider、第三方 series bootstrap、K 线字段投影和 pair refresh 接入 runtime deps。
4. 将订单、钱包、backtest strict/range/clock、scheduler 和 entry 生命周期接入实例依赖。
5. 固定 banexg `v0.2.64`，删除本机 replace，并补充 runtime、storage、series、lifecycle 和隔离回归。

## 剩余工作与验收

- R1 的确定性 typed backtest/live 双 runner 场景已经通过：同时推进、独立取消、账户/价格/时钟/策略/订单结果与串行基线一致，并通过 race。仍需在有真实 PostgreSQL/QuestDB、固定行情 fixture 和生产交易所的环境做完整 runner 演练。
- 继续迁移仍使用 package global 的兼容调用方（策略 job、订单/钱包、交易所默认 session、ORM facade、RPC/Web 和后台 worker）；迁移完成后再删除 legacy gate 及重复构造变体。这是后续兼容层收敛，不是本轮 High/Medium 修复缺口。
- 在有 PostgreSQL/QuestDB 和固定行情 fixture 的环境执行异库隔离、同库 SID/WAL 协调及完整 ORM 回归；外部 fixture 缺失时单独记录，不冒充通过。
- 每次变更先补行为回归，再运行受影响包测试、`go test ./... -run '^$' -count=0`、race、`go vet ./...`、`go build ./...`、`go mod download` 和 `git diff --check`。

banbot 工作树不自动提交；依赖 tag 的发布在 banexg 仓库完成后，banbot 只引用可从 Go proxy 获取的精确版本。
