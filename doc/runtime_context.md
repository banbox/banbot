# BanBot Runtime 上下文架构

本文记录当前工作树已经实现的性能优先 `Process -> Runtime` 架构。它描述现状：High/Medium 的主要修复已落地，但不把 typed state、legacy gate 或局部双实例测试解释为完整的多 Runtime 业务并发隔离。

剩余 legacy 边界集中于 spider、数据维护、旧 dev Web 与维护工具；因此 LegacyState/legacygate 还保留，不能宣称全部全局状态已删除。本文原有描述与新实施记录冲突时，以实施记录及当前源码为准。

## 1. 核心结论

- `runctx.Key[T]` 和 btime Context helper 已删除。当前没有调用方需要通用 typed Context key；保留它会鼓励动态 service locator 和热路径查找。
- 不实现 goroutine-local、`CurrentRuntime`、goroutine ID 映射或运行时 hack。
- `context.Context` 只传递取消、deadline 和 I/O 生命周期。ORM series 的表锁由 repo 内部显式获取和释放，不通过 `Context.Value` 传递重入标记。
- [`runtime.Process`](../runtime/runtime.go) 只持有 Runtime ID 计数器、按存储身份共享的 `*orm.SIDAllocator`，以及按 URL 复用的低频 `*orm.SymbolSIDRegistry`；[`runtime.Runtime`](../runtime/runtime.go) 组合各领域包拥有的 typed state。
- legacy facade 仍是进程级兼容路径。统一 gate 只保证这些路径串行，不提供完整的多 Runtime 并发隔离。

## 2. Process 与 Runtime 所有权

同一 `Process` 创建的 Runtime 共享 `SIDAllocator`，避免某个 `SymbolState.Reset` 后复用已分配 SID。allocator 只保存最大 SID、逻辑 identity 到 SID 的 reservation，以及必要的分配串行区；它不保存 symbol metadata、订阅、recovery 目录、数据库连接或配置。

显式 Runtime 的 metadata lock root 与 allocator 的 storage identity 一致，legacy facade 才回退到包级配置推导的 root。一个 allocator 在已经产生本地 SID reservation 后不能再切换到 PostgreSQL registry；同一 registry URL 的 `AutoCreate` 策略也必须一致，避免构造顺序改变 authority 行为。

SID 权威按存储后端区分：Timescale/PostgreSQL 的 `exsymbol.id` 使用 `exsymbol_sid_seq`，并由 `(exchange, market, symbol)` 唯一约束和 `INSERT ... ON CONFLICT ... RETURNING` 保证逻辑 identity；QuestDB 配置 `Database.SIDRegistryURL` 时，独立 PostgreSQL registry 的 sequence、logical primary key 和稳定 `write_ts` 是跨进程/跨主机权威，QuestDB 只保存物理目录。`SIDAllocator` 在这两种路径中都是进程内 reservation/fence 和 recovery 协调器，不是跨主机权威。

QuestDB 未配置 `SIDRegistryURL` 时保留同机 single-writer 兼容路径：本地 lease 和 pending marker 可以保护单主机写入，但不能把多个主机安全地合并到同一 SID 空间。需要多 writer 时必须显式配置共享 PostgreSQL registry；registry schema 的自动创建/sequence 对齐只在 `Database.AutoCreate` 开启时执行。

registry 首次接管已有 `exsymbol_q` catalog 时，新增逻辑 symbol 会先读取并验证物理 catalog 的最大 SID，再把 registry sequence 推到该 SID 之上，然后才进行 reservation。请求中已存在的 physical row 仍通过 `Adopt` 保留原 SID；这不是全量扫描，因此跨部署启用 registry 前仍应先完成 catalog 校验。

`Runtime` 当前组合：

| 字段 | 当前职责 |
| --- | --- |
| `Core` | `core.State`：运行模式/环境、pairs、订单匹配周期、运行和取消状态 |
| `Config` | `config.Snapshot`：构造时深拷贝的配置与目录 |
| `Clock` | `btime.ClockState`：实时或模拟时间 |
| `Market` | `com.MarketState`：价格和复制进度 |
| `Symbols` | `orm.SymbolState`：symbol 索引、identity、订阅和 recovery root |
| `Storage` | `orm.Storage`：显式连接池、数据库后端和存储协调身份 |
| `Batch` | `strat.BatchState`：batch 队列和 `LastBatchMS` |
| `Strategies` | `strat.State`：策略实例、jobs、订阅字段、性能状态与 pair hooks |
| `Orders` | `ormo.OrderState`：任务、活动订单、历史订单与执行选项 |
| `Trading` | `biz.TradingState`：订单管理器、钱包、快照调度状态 |
| `Cron` | 实例 scheduler，构造时绑定语言和时区 |
| `Notifications` | `rpc.Session`：实例通知通道和远程命令依赖 |
| `Exchange` | 显式交易所 session；关闭责任由构造入口承担 |

领域类型仍由原包定义，`runtime` 负责构造、组合和关闭。`Runtime.Close` 先停止调度、通知及注册的后台任务，等待 callback/join，再清空 `Market`、`Symbols`、`Batch`、`Strategies`、`Orders`、`Trading` 并关闭 `Core`。`Storage` 和 `Exchange` 是显式依赖，不能因为一个 Runtime 关闭就误关另一个 Runtime 共享的连接；创建资源的入口负责关闭，`Process.Close` 释放其持有的 SID registry。

## 3. Legacy gate

[`runtime.WithLegacy`/`LockLegacy`](../runtime/legacy.go) 使用同一个进程级互斥量，保护仍会使用或安装 package globals 的兼容调用链。

当前覆盖范围：

- backtest、trade、优化与报告入口已走显式 Runtime，不获取该 gate；旧 dev Web/API 与维护工具仍需兼容 gate；
- entry 数据路径：repair/verify/correct/adjust、series download、spider、load、aggregate、init、import、export，以及同类通过 `runConfigCommand` 执行的配置型命令；
- [`runtimeplan.Inspect`](../runtimeplan/inspect.go)，因为它仍临时安装并恢复 `config`、`core` 和 `btime` 状态。

Cobra 的 legacy 配置型命令在 `runConfigCommand` 外层取得 gate，内部调用不加锁的 helper，避免同一 goroutine 重入互斥量；直接导出的 legacy entry API 则由各自 wrapper 取得同一 gate。显式 Cobra runner 通过 `openExplicitEntrySession` 创建自己的 Process、Storage、Exchange、Config 和 Runtime，不要求 `LegacySession`。一次显式 backtest/trade session 内创建一个 `Process`，该 session 内的 Runtime 共享 SID allocator；仍使用 legacy facade 的 session 继续串行化。

以下 pure paths 不需要 gate：命令树构造、参数/legacy flag 规范化、help/version、`series list` 对已注册定义的只读 JSON 输出，以及 `runtimeplan.DecodeRequest` 的纯解码。它们不安装运行期全局状态。gate 不进入 bar、tick、价格或策略 callback 热路径。

## 4. Live admission、stop 与 join

live runner 当前建立了三层停机边界：

1. `CryptoTrader` 通过 `Runtime.OnClose` 注册 stop hook。Runtime stop 后先禁止新的延迟 batch callback，并关闭 `LiveProvider`。
2. `LiveProvider.Close` 先关闭 provider handler admission，再调用 socket `ClientIO.Stop`，最后等待已经接纳的 series handler。
3. `BanConn.Stop` 禁止 socket handler admission 和重连，关闭活动连接，并等待已经接纳的 listener、fallback 和 bad-message handler。provider handler 内再启动的 nested callback 因此也在 join 范围内。

`CryptoTrader.runWithDeps` 返回前还会等待已接纳的延迟 batch callback。entry 的顺序是 `t.Run()` 返回后才执行 deferred `rt.Close()`，因此正常 runner 路径会先 stop/join provider、socket 和 trader callback，再 reset Runtime-owned state。该保证依赖当前 runner wiring；`Runtime.Close` 本身并未拥有所有 live goroutine，不能单独证明任意嵌入调用都已 join。

## 5. 交易所语义与热路径

[`core.SplitSymbol`](../core/utils.go) 不再读取 `core.ExgName`，也没有 China 特判；默认 parser 只处理通用分隔符语法。Runtime-owned `Market.Prices` 内持有 [`core.SymbolParser`](../core/symbol_parser.go)，可在构造边界注入 typed `SymbolParserStrategy`。

交易所价格语义由 `banexg v0.2.64` 的 capability 和 `MapMarket` 提供。banbot 在构造边界绑定 adapter parser，不按交易所名称补写专属 quote/settle 或合约月份逻辑。旧 `com` facade 按 exchange 保存独立 `PriceState`，保留基础品种别名和切换交易所后恢复旧价格的兼容行为。

parser 使用并发缓存和 hot/warm 原子指针；exchange adapter 只在 cache miss 调用。价格和 runner 热路径继续使用具体 receiver、字段和局部变量，不通过 Context 查找服务。

配置解析和旧 K 线导出不再按 exchange name 写分支，而是在低频边界读取 banexg adapter capability；导出任务构造时把辅助字段格式能力固化到任务，避免进入 K 线循环后再次探测。adapter 未提供该能力时仍使用通用 slash-delimited pair 规则。

## 6. QuestDB、任意时序列与 SID recovery

`orm.DataSeries.Values map[string]any` 仍是统一数据模型。默认 K 线和自定义 series 不引入 typed OHLCV 快速路径；扩展字段、字段类型、缺失值和显式 `nil` 都按 schema 写入并以 SQL NULL 读回。

QuestDB K 线 CTAS 和 generic series rewrite 先从 `table_columns()` 捕获原表 schema，再按实际列清单执行 CTAS。快照包含列名/类型/designated/upsert-key、每 SID 行数和样本值；临时表通过 WAL 轮询达到同一快照后，才进入旧表备份、临时表激活和再次验证。验证或 rename 失败时不删除旧表；只有新表激活并复验成功后才删除 backup。该流程保留不在静态 K 线字段列表中的扩展列及其类型/NULL，而不是只比较总行数。

QuestDB 写后读仍遵守以下规则：只有 `pgx.ErrNoRows` 等“尚不可见”状态可轮询，连接、扫描和其他硬错误立即返回；超时保留 pending/recovery 状态；依赖 INSERT、CTAS 或 metadata 写入的后续操作先等待预期时间戳、范围、覆盖或快照可见。

exsymbol pending marker 在原子 rename 发布后同步 recovery 目录。后续 reconciliation 先校验 marker；损坏 marker fail-closed，不继续分配 SID。每个有效 marker 的 SID 在 identity 过滤前先进入 allocator fence，已确认行再缓存到对应 `SymbolState` 并建立 identity reservation；未确认或 foreign marker 保留，全部确认后才删除。recovery root 属于 `SymbolState`，SID reservation 属于 Process 共享 allocator；使用 registry 时，跨进程 identity reservation 先落 PostgreSQL，再以稳定 `write_ts` 重试 QuestDB WAL 写入。

## 7. Runtime runner 已消费与剩余边界

entry 的 trade/backtest/optimize/bt-opt/sim-bt/test-pickers/collect-opt/bt-result/bt-factor 均接入显式状态；优化工厂负责每轮创建和释放 Runtime，报告不临时安装任何全局状态。构造 API、订单事件时间、logger 和 registry 的迁移说明见实施记录。

`biz.RuntimeDeps.DataDeps()` 是统一数据依赖投影。Symbols、Storage、Catalog、Callbacks 和时钟跟随同一 owner；relay 使用私有核心、策略、订单和交易状态，借用父任务的外部资源。runner 不再提供部分状态/legacy 构造变体。

显式 live 的 HTTP/API/auth 和 WebSocket 使用实例状态，WebSocket 慢客户端的监控发送有界且不阻塞交易循环；关闭等待所有接纳的 handler/writer 后完成。回调、provider、调度器的 stop/join 保护仍保留。

尚在 legacy gate 内的是真实维护入口，包括 spider、数据/K线维护、旧 dev Web 与维护工具。这些仍需要逐入口迁移，随后才能删除 LegacyState 和对应 facade。当前代码及测试不能作为两个真实生产数据库 runner 并行 E2E 的替代证据。

## 8. Context 约束

Runtime context 用于取消树、deadline、startup/第三方数据源调用、socket 等 I/O。callback 需要实例状态时，通过构造参数、具体 receiver、窄接口或闭包绑定。

ORM series table 的重入锁标记已从 `context.WithValue`/`Context.Value` 删除。repo 方法在需要时显式取得 process/table lock，并在同一方法边界释放；嵌套的 locked helper 只接受调用方已经建立的边界，不从 Context 推断状态。仓库其他独立功能若使用 Context 元数据，不代表 Runtime/ORM 可以恢复 service locator 模式。

## 9. 既有设计决策与历史验证证据（本次验证见实施记录）

- `Key[T]`：删除，无 alias、wrapper 或兼容层。
- `banexg`：固定 `github.com/banbox/banexg v0.2.64`，`go.mod` 无本机绝对路径 replace，模块可从 Go proxy 下载。
- 性能：typed hot paths 保持具体 receiver；现有 symbol parser、price state 等代表性 benchmark 报告零分配。该证据不是全链路 benchmark，也没有证明所有路径均低于 5% 回归阈值。
- 历史账本基线：旧策略工作树快照的配置 SHA-256 记录为 `87bed31c92dd4769bf17ae218181c958f984fd3b5d63983c3141e4a4b9484a7b`；当时的 `Total Orders/BarNum = 733/1568085`、`Final Balance = 693.79`。
- 2026-09-06 复验：在 `/data/quant/strat1` 运行 `./bot backtest -config @adv.yml`，退出码为 0；`Total Orders/BarNum = 0/1626569`、`Final Balance = 3000.00`、`Total Profit = 0.0%`、`Total Fee = 0.00`；elapsed `2:25.27`、user `53.70s`、最大 RSS `1064760 KB`，输出目录为 `/data/ban/data/backtest/18fbb39e60`。
- 可比性：本次复验使用的 `/data/quant/strat1` 策略工作树状态不同于历史账本对应的旧快照，因此两次结果不能直接比较，也不得表述为当前指标与历史基线一致。
- 变更后验证：`go vet ./...` 和 `go build ./...` 均通过；定向 package 测试和 race 测试均通过。`go test ./orm -count=1` 仍受外部 QuestDB WAL 可见性和 Binance linear `MATIC/USDT:USDT` market fixture 缺失影响，`TestBulkDownOHLCV` 未能完成；这属于外部环境限制，不改变已通过的编译、静态检查和 Runtime 回归证据。代表性 benchmark 已运行，typed hot paths 的代表性 benchmark 为零分配。
- 测试：已有回归覆盖 legacy gate 互斥、runner typed deps、双 Runtime 局部状态隔离、第三方 source/字段投影隔离（相同 SID/timeframe、全局第三组 job、显式 NULL）、live provider/socket admission-stop-join、Context.Value 移除、QuestDB WAL 轮询/硬错误、CTAS 删除前验证、series NULL、recovery marker 和 SID reservation。

这些证据支持当前迁移切片，不支持“完整多 Runtime 并发隔离已经完成”的结论。

## 10. 剩余风险

- legacy globals 仍覆盖策略、jobs、订单、钱包、交易所 session、ORM pool 和若干后台 worker；gate 是当前正确性边界，也是并发能力限制。
- 生产 typed live 的主要 cron、订单/钱包循环与 Web 已接入实例生命周期；仍应对外部组件和自定义嵌入路径验证 stop/join，不能推断任意外部 goroutine 都自动归属 Runtime。
- 未配置 registry 时，Process 内 allocator 和本地 lease 只协调同一主机的 single-writer；配置 registry 后 PostgreSQL 的 sequence/unique key 才是跨进程 SID 协调边界。
- QuestDB snapshot 通过 schema、每 SID 行数和样本验证，不是逐单元格全表校验；WAL timeout、rename 中断和 backup 恢复仍需运维可见性。
- 最终多 Runtime 并发承诺仍需迁移剩余 globals、删除临时全局安装，完成两个完整 runner 的确定性并发/取消/结果一致性验证，并持续运行全量测试和稳定 benchmark 对比。
