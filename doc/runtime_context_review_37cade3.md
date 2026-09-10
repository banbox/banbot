# RuntimeContext 重构整体 Review（37cade3）

审查日期：2026-09-08；实施复核日期：2026-09-09。审查对象：`37cade3d80147755c558f12043f74f99639841c9` 相对父提交的改动。本文保留原始审查证据和建议；当前工作树的完成状态以“当前实施状态”和最后的验证记录为准。执行范围和验收条件见 `runtime_context_implementation_plan.md`。

## 1. 结论

**方向正确；High/Medium 的实现修复已落地，显式 backtest/live runner 已通过确定性并发、取消隔离和 race 验收。**完整真实数据库 runner、生产交易和跨主机存储演练仍属于环境验收，当前环境不能据此宣称已经完成。

原始独立 code-reviewer 建议 **REQUEST CHANGES**，独立架构审查结论为 **BLOCK**；该结论针对提交当时的状态，不是对本轮修复的重新否决。本轮已用确定性 typed runner 覆盖双 runner 并发、取消和串行基线一致性；尚未完成的是依赖真实 PostgreSQL/QuestDB、固定行情和生产交易所的完整端到端演练。

当前最有价值的设计是：各领域包保留自己的具体 State 类型，`runtime.Runtime` 在上层组合，通过构造参数把依赖绑定到业务实例。没有必要改成通用 Context 容器，也没有必要把所有领域类型搬到一个新包。

主要剩余风险在迁移边界：同一业务仍有实例状态和 legacy 全局状态两套入口，旧导出 API 仍由全生命周期 gate 串行保护；显式 CLI runner 已使用独立 Runtime/Storage/Exchange/Config，局部 runner 验收通过，但真实数据库和生产服务的完整并发演练仍受环境限制。

审查采用业务正确性、架构独立审查及 ORM 专项抽查。提交涉及 199 个文件、约 4.2 万新增行；本文聚焦状态所有权、运行入口、热路径、生命周期和存储安全调用链，不声称逐行证明所有改动正确。问题分为可直接从代码确认的缺陷/目标缺口，以及需要基准或专项测试才能决定的优化建议。

## 2. 对三个核心原则的判断

| 原则 | 判断 | 建议 |
| --- | --- | --- |
| 强类型、高性能 | 组合根及依赖字段总体符合；读取热路径有零分配证据，不能推导所有路径零分配或无性能回退 | 保留具体 State 指针；构造时校验和绑定，循环内直接使用 receiver/字段 |
| 保持包依赖 | 总体合理，生产代码目前只有 entry 直接导入 runtime；下层领域包没有反向依赖组合根 | 继续由 entry/runtime 装配，通过已有领域类型及必要的窄接口跨边界 |
| 简单、优美、可维护且彻底解耦 | High/Medium 主路径已完成；兼容分支仍是有边界的迁移层 | 继续迁移调用方后删除旧路径，不再横向增加 WithX 构造函数 |

这里需要区分三种 map：

1. 用 `map[string]any` 或 `context.Value` 查找运行状态/服务：不采用。
2. `map[string]*BanWallets`、按交易对索引策略等强类型业务集合：按业务基数动态索引是合理的，不能仅因为使用 map 就认定违反强类型；固定依赖仍应是结构体字段。
3. `orm.DataSeries.Values map[string]any`：这是任意时序字段的既定数据契约，必须保留，不能为了零分配换回固定 OHLCV 模型。

`context.Context` 继续只负责取消、deadline 和 I/O 生命周期。快照构造阶段的深拷贝或反射，与每根 bar 上动态查找服务不是同一类性能问题。

## 3. 已确认问题与拟改动

| 优先级 | 编号 | 问题性质 |
| --- | --- | --- |
| HIGH | R1 | 正式 runner 仍串行，目标能力缺口 |
| HIGH | R2 | 实例存储身份与全局连接脱节 |
| HIGH | R3 | 依赖未固定，干净构建不可重现 |
| HIGH | R6 | 失败恢复路径丢失 swap intent |
| HIGH | R9 | 第三方订阅与扩展字段投影仍使用全局策略 registry |
| HIGH | R10 | 回测范围与严格执行模式未消费实例配置 |
| MEDIUM | R11 | 实例订单清理仍改写全局历史订单 |
| MEDIUM | R4 | entry 没有关闭所创建的 Process |
| MEDIUM | R7 | 默认 scheduler 读取全局配置 |
| MEDIUM | R8 | 构造失败漏取消子 context |
| MEDIUM | R5 | 架构文档与实现不一致 |

### 当前实施状态（2026-09-09）

下表覆盖本轮对 R1–R11 的处理；各节随后保留原始证据、影响和建议，便于追溯，不应再解读为“尚未修改”。

| 编号 | 状态 | 当前结果 |
| --- | --- | --- |
| R1 | 主要修复完成/测试验收通过 | 显式 Cobra `backtest`/`trade` 通过 `openExplicitEntrySession` 创建独立 Runtime，不要求 `LegacySession`；旧导出兼容入口继续受 legacy gate 保护。`opt`/`live` 的确定性双 runner 并发、独立取消和串行基线测试已通过；真实数据库、固定行情和生产交易所的完整 runner 演练仍待环境验收。 |
| R2 | 已完成主要修复 | `orm.Storage`、显式 `Queries`/`SeriesRepo`/`SymbolState` 绑定存储身份；显式路径缺少 SID 不再回退进程级 registry。异库端到端数据库演练仍受外部服务约束。 |
| R3 | 已完成 | `go.mod` 固定 `github.com/banbox/banexg v0.2.64`，删除本机绝对路径 `replace`；模块可由 Go proxy 解析。 |
| R4 | 已完成 | entry session 在正常、错误和构造失败路径关闭 `Process`；legacy gate 在 `Process.Close` 完成后才释放。 |
| R5 | 已完成 | 本文、实现计划和运行时架构文档已按当前 ownership、legacy 边界和验证限制更新。 |
| R6 | 已完成 | QuestDB rename/drop 失败时仅在清理成功后清除 swap intent；失败保留 recovery marker，并有专项回归。 |
| R7 | 已完成 | scheduler 显式接收 location/lang；Runtime 不读取进程级时区/NTP 配置。 |
| R8 | 已完成 | Runtime 构造失败统一关闭 Core/取消子 context，并有失败路径回归。 |
| R9 | 已完成 | 第三方 source 收集和 K 线字段投影使用 `strat.State`；双 State、同 SID/timeframe、全局第三组 job 的隔离回归已补齐。 |
| R10 | 已完成 | backtest 的范围、strict、clock、输出目录和 scheduler 从实例 deps/config 读取；旧兼容 API 仍明确保留 legacy 分支。 |
| R11 | 已完成 | 订单历史清理绑定 `ormo.OrderState`，实例 manager 不再改写 legacy 历史 registry。 |

以下各节保留原始审查时的证据、影响和拟改动，便于追溯；当前是否完成以“当前实施状态”和“本次验证”为准。

### R1 · HIGH：正式入口的并发能力与迁移边界

**原始证据：** `entry/entry.go` 的旧 `runLegacyRunnerSession`、`runBackTestSession` 和 `runTradeSession` 会进入 legacy gate；旧 `opt.NewBackTestWithRuntimeDataDeps` 也要求有效 `LegacySession`。本轮新增的 `entry/runtime_entry.go:32` `openExplicitEntrySession` 和 `opt.NewBackTestWithRuntimeDataDepsOwned` 已把显式入口移到独立 `Process`、`Storage`、`Exchange`、`Snapshot` 和 `Runtime`，不再要求该 session。

**原始触发/影响：** 同一进程通过旧兼容入口运行长期实盘和回测时仍会等待 gate；这是兼容层的明确限制。显式入口不再安装该 gate，typed runner 测试已证明 sibling 取消不会影响另一任务。绕过旧 gate 仍不能替代真实服务依赖的隔离，因此不能把兼容入口直接改成无锁并发。

**本轮改动及后续边界：** 保留 gate 保护尚未迁移的兼容路径；显式 runner 已闭合配置、策略、订单、钱包、存储、scheduler 和后台 callback 的 typed 依赖，并在构造阶段 fail-closed。后续只需继续迁移仍由兼容 API 使用的调用方，待调用方全部切换后再删除 gate 和重复构造变体，不能先删锁再补状态隔离。

兼容边界仍包括旧 backtest/live facade、legacy `config/core/btime` 状态、默认交易所 session、ORM facade 和进程级 RPC/Web handler；显式分支不会回退到这些状态。后续迁移应逐个确认实际消费者，不能只在 Runtime 上增加同名字段就算完成。

**验收结果：** `opt` 和 `live` 已用确定性行情/模拟交易所同时推进两个 typed runner，验证同时有进度、独立取消、账户/价格/时钟/策略/订单结果与串行基线一致，并通过所涉包的 race 检查。依赖真实 PostgreSQL/QuestDB、固定行情 fixture 和生产交易所的完整 runner 验收仍需在相应环境执行。

### R2 · HIGH：实例 symbol 身份与实际数据库连接并未绑定

**证据：** `orm/exsymbol.go:370` 新增的 `(*SymbolState).EnsureSymbols` 在 `orm/exsymbol.go:393` 调用 `Conn(nil)`；`orm/base.go:306` 从全局 `pool` 取连接。`orm/base.go:42` 和 `orm/base.go:57` 分别保留 pool 与 `IsQuestDB`，`orm/base.go:109` 的 setup 会关闭旧 pool 并重建。`orm/pub_meta_queries.go:466` 等显式 state 可到达的代码仍按全局 `IsQuestDB` 选 SQL。

**触发/影响：** Runtime A/B 可以有不同的配置快照和 SID allocator namespace，但 `A.Symbols.EnsureSymbols` 仍可能访问最近 setup 的数据库 B。只创建 Runtime 而未 setup ORM 的嵌入调用还可能在 nil pool 上失败。`SymbolQueries` 绑定了缓存状态，但没有让存储后端和连接所有权成为同一绑定。不同数据库或不同 SQL 后端并行时，缓存隔离不足以保证数据隔离。

**拟改动：** 在 orm 内建立一个最小具体存储 owner，持有连接池、后端种类及存储协调身份；在装配边界显式传给需要持久化的 owner/repo。复用现有 Queries 和 SeriesRepo，不另建通用服务注册器。按存储身份可以共享连接池和写入协调锁，但任务不能重设或关闭其他任务的连接。不要把协调锁机械地改成每个 Runtime 一把锁，那会破坏同表写入互斥。

**验收：** 两个不同数据库的 Runtime 同时注册同名 symbol、读写 series，实际 SQL 只能命中各自连接；同一数据库的两个 Runtime 仍共享正确的 SID/表替换协调。无存储绑定时构造或调用应返回明确错误。

### R3 · HIGH：构建依赖本机绝对路径和外部未提交接口

**证据：** `go.mod:5` 将原本注释的 replace 改为 `/data/ban/banexg`。`orm/base.go:732` 使用 `banexg.SymbolScopedMarketLoader`。审查机器上的 banexg HEAD 是 `68aefbebb9a8b37766a75b5cf0f0c5a2ca03a364`，提供该接口的 `market_capability.go` 为未跟踪文件，另有 `china/biz.go` 未提交修改。当前缓存的 `banexg@v0.2.63` 没有该接口。

**影响：** 本机编译成功无法证明提交可在干净环境重现；其他开发者没有该目录会直接失败，仅删除 replace 也不够。交易所 capability 放在 banexg 的方向正确，但依赖交付不完整。

**拟改动：** 先在 banexg 固定并交付所需 capability，再在 go.mod 引用可取得的精确版本；本地联合开发使用本机 go.work 或不提交的覆盖配置。继续让交易所专有语义留在 banexg，禁止为解决编译问题在 banbot 补交易所名称分支。

**验收：** 在没有 `/data/ban/banexg`、没有未提交依赖文件的环境完成模块解析、build、vet 和测试。

### R4 · MEDIUM：entry 创建的 Process 没有对应 Close

**证据：** `entry/entry.go:27`、`:34`、`:43`、`:136` 创建 Process；实际 runner 在 `entry/entry.go:96` 和 `:159` 只关闭并 Join Runtime。entry 内没有调用 `process.Close()`。`runtime/runtime.go:137` 才负责释放 Process 持有的 SID registry，而 `orm/sid_registry.go:117` 的 registry 会懒创建 pgx pool，并保存其关闭函数。

**触发/影响：** 在长驻进程中反复执行 entry session，且配置并实际使用 SID registry 时，Runtime 结束不释放对应 registry pool。单次 CLI 随进程退出可能不显眼，但“一个进程创建很多任务”的使用方式会累计连接及后台资源。仅构造而从未访问 registry 时，不声称已经泄漏数据库连接。

**拟改动：** 在创建 Process 的 session owner 处保证 Close，覆盖正常返回和初始化失败；复用 Process 的内部子回测不能擅自关闭它。释放动作应完成于该 owner 的 session 边界内。用 registry 的可注入 open/close hook 验证多次 session 每个 pool 恰好关闭一次。

### R5 · MEDIUM：架构说明明显落后于同一提交的实现

**证据：** `doc/runtime_context.md:36` 声称 Runtime 尚不拥有策略图、订单和钱包；实际 `runtime/runtime.go:214` 已有 `Strategies`、`Orders`、`Trading`、`Cron`。原文 lifecycle 与剩余全局状态清单也混合了较早迁移阶段的描述。`biz/trader.go:39` 的依赖注释同样仍称这些状态 package-scoped。

**影响：** 后续维护者无法准确判断哪些已隔离、哪些依赖 gate，可能继续增加重复 state 或提前移除保护。原文“代表性 hot paths 零分配”还容易被误读为包含价格写入，本次测量并非如此。

**拟改动：** 更新现状文档和直接关联的 ownership 注释，按“已实例化 / 仍依赖全局 / 可按存储身份共享”列状态清单；验证记录写明代码与外部依赖版本、实际测试结果及基准范围。本文不会把旧文档的历史测试记录当成当前验证结果。

### R6 · HIGH：QuestDB 替换失败分支会在清理失败后仍删除恢复意图

**证据：** `orm/questdb_visibility.go:1856` 和 `orm/compact.go:703` 在源表 rename 返回错误后，调用 `cleanupQuestRewriteFailure` 清理临时表，随后无条件调用 `clearQuestRewriteSwapIntent`。清理失败只被拼接进错误，没有阻止 marker 删除。对照这两个函数的下一条“激活失败”分支，临时表删除失败时会立即返回并保留 marker，前后契约不一致。

**触发/影响：** 同时模拟 `RENAME source -> backup` 失败且 `DROP temp` 失败，源表未动、临时表遗留，但 swap intent 已删除。下一次恢复无法通过这条 intent 发现遗留临时表。此处确认的是恢复信息丢失，不声称这个场景必然已经丢失源表数据。

**拟改动：** 分开处理原始 rename 错误和临时表清理结果；只有清理成功后才移除 intent。清理失败时保留 marker 和表名供后续恢复。通用表替换与 compact 两条路径同时修正，避免只修一个调用点。

**验收：** 各补一个精确故障注入回归：rename 失败、drop 也失败，断言源表保留、临时表仍存在、intent 仍可读取；恢复后能够正常清理。按项目规则，这类 WAL/表替换修复必须带专项回归。

### R7 · MEDIUM：默认 scheduler 的配置仍取自进程全局

**证据：** `runtime/runtime.go:353` 用 `opts.DisplayLocation` 创建任务时钟，但默认 scheduler 使用 `com.NewScheduler()`；`com/cron.go:27` 内部读取 `btime.LocShow`、`bntp.LangCode`，并修改进程级 slog 级别。

**影响：** scheduler 对象虽然独立，定时规则的时区仍受全局配置控制。改变 Runtime 的时区并不改变默认 cron 时区；即使显式传入自定义 Scheduler 可规避，默认构造路径仍未解耦。显示时区与调度时区是否一致应定义清楚，不能隐式读取另一套全局值。

**拟改动：** 在 com 的构造边界显式接收 location/lang（必要时使用独立的调度时区选项），Runtime 从自己的配置装配；进程日志配置只在进程入口初始化。添加不同全局时区、不同任务时区下相同 cron 表达式的 next-fire 测试。

### R8 · MEDIUM：Runtime 构造的 symbol 校验失败路径漏掉 cancel

**证据：** `runtime/runtime.go:312` 调用 `core.NewState(opts.Context)`，其内部创建子取消 context；随后 `runtime/runtime.go:326` 校验 pair 失败直接返回，没有 `coreState.Close()`，而稍后的 allocator/recovery 错误分支会关闭它。

**影响：** 当 parent 为长驻可取消 context 且反复尝试无效配置时，这些失败构造的子 context 会留在 parent 的取消树中，直到 parent 结束。普通 Background parent 的保留行为不同，不能笼统声称所有失败都会启动或泄漏 goroutine。

**拟改动：** 最简单是把纯 symbol 校验移到创建 coreState 之前；或统一构造失败清理，成功移交所有权后解除清理。保持成功 Runtime 的取消树不受影响，并补失败构造覆盖。

### R9 · HIGH：第三方订阅与扩展字段投影仍查全局策略 registry

**证据与两条具体路径：**

- `live/crypto_trader.go:526` 的第三方 source bootstrap 调用 `runtimeJobs()`，后者在 `:539`、`:545` 扫描全局 `strat.AccJobs`，没有选择 `t.RuntimeDependencies().Strategies`。`collectJobsFn` 是可覆盖钩子，但生产代码没有给它接入实例 registry。
- `strat/data_subs.go:137` 的 `CollectKlineSubFieldsWithSymbolState` 虽传入实例 Symbols，`:141`、`:153` 仍扫描全局 `AccInfoJobs`。`data/feeder.go:1512` 和 `data/provider.go:1262` 的显式 feeder/字段补全路径仍调用它。

**触发/影响：** A 的私有策略有第三方订阅或自定义 K 线字段，而全局 registry 为空或属于 B：A 会漏启动自己的 source、漏查询自己的扩展列，或按 B 的 job 生成订阅/投影。即使暂时串行、全局残留恰好掩盖问题，也不能据此认定实例路径正确。保留 Values 的类型并不自动保证投影字段来源正确。

**拟改动：** 将 job 收集及字段投影收口为 `strat.State` 的方法，显式路径使用自身 Strategies + Symbols；旧 facade 只转发 legacy state。复用已有 `data.RuntimeDeps.Strategies`，不要再增加一个动态 registry 或只传更多 Symbols 参数。

**验收：** 两个 State 使用同 sid/timeframe，各订阅不同扩展列和第三方 source；全局 registry 故意放第三组 job。断言实际 source 启动集合、查询列以及最终 Values 只由各自任务决定，包含缺失字段与显式 NULL 的情况。

### R10 · HIGH：显式回测的配置和严格模式仍由全局决定

**证据：** `opt/backtest.go:479` 接收 RuntimeDeps，却进入共享 `newBackTest`；`:495` 的 getEnd 闭包读 `config.TimeRange.EndMS`，`:499` 使用全局配置哈希和目录，`:511` 的 Init 用全局起始时间。`biz/trader.go:867`、`:872` 在处理实例 job 时调用 `config.StrictBacktest()`，而 `config/biz.go:760` 读取的是全局 `core.BackTestMode` 与 `config.Data.BTStrict`。

**触发/影响：** 即便在有效 legacy session 内，显式传入配置 A、全局保留配置 B，回测开始/结束边界仍取 B；严格配置 A 也可能因全局为 live/non-strict 而不排序或启用并行执行。这不只影响未来真正并发，也影响目前显式构造 API 对独立快照的语义。

**拟改动：** 在回测构造边界从 deps.Config/Core 绑定时间范围、输出根目录及 strict 标志，回调直接读这些具体字段。将仍需 legacy 的分支明确限于旧入口。严格模式不应在每根 bar 上再次查询全局配置。

**验收：** 保持全局 B 不变，分别传入 A/B 时间范围和相反 strict 配置，检查实际事件边界、执行顺序及 parallelOnBar 决策。固定行情下订单结果应可重现。

迁移同一调用链时还应处理 `strat/main.go:1087` 的全局时钟以及 `goods/filters.go:83` 对全局 `core.BanPairsUntil` 的写入：实例选币必须使用任务时间和任务禁入状态。此处的 `btime.TimeMS()` 不一定总是主机时间，但在全局 live、实例 backtest 时会选错时钟。

### R11 · MEDIUM：实例订单管理器收尾仍改写全局历史订单

**证据：** `biz/odmgr_local.go:825` 的 CleanUp 前半段通过 `o.openOrders()`、`o.priceNow()` 使用实例状态，但 `:910` 至 `:917` 无条件过滤并重写 `ormo.HistODs`，没有使用绑定的 OrderState。

**触发/影响：** 清理实例 A 会修改 legacy 的历史订单，同时 A 自己的历史 registry 未执行这一步“剔除未成交订单”的过滤。全局历史为空时，遗漏可能不被现有实例测试发现。

**拟改动：** 将历史过滤放进 OrderState 的具体方法，实例 manager 使用绑定 state，旧 manager 使用 legacy state；保持锁在 registry owner 内。测试分别预置 A/B/legacy 历史订单，只清理 A，确认仅 A 被过滤。

## 4. 建议保留的设计与可简化之处

### 4.1 保留领域 State 与上层组合

建议继续使用 `Runtime -> core.State / config.Snapshot / ClockState / MarketState / SymbolState / strat.State / OrderState / TradingState`。具体 State 的方法留在原包，领域包不导入 Runtime；业务对象只接收自己会消费的具体依赖。

“统一 context”应该体现为一个任务的一致所有权，不要求每个函数都传整个 Runtime。构造时绑定一次，bar/tick/订单处理时使用对象已有字段即可。已有 `RuntimeLifecycle`、`CallbackTracker` 是实际跨包需求，可以保留，不需要为每个具体 State 再造接口。

`config.Snapshot.View()` 目前通过约定只读，而 Go 类型本身仍可变。保留构造时深拷贝与清晰的只读约定，先检查真实写入者；高频业务只绑定自己需要的配置字段。没有具体修改冲突证据时，不建议为了模拟不可变类型而批量增加 getter 或每次读取再深拷贝。

### 4.2 兼容应收敛到入口，不应决定新 State 的内部表示

`orm/ormo/state.go:14` 为引用旧 globals 使用了大量 `*map`、`*slice`、`*int64` 和锁指针。这仍是强类型，不是动态容器，但每个新实例都要承担旧 API 的间接层和初始化复杂度。

拟在旧变量调用者迁移完成后，把它们改成直接的 map/slice/标量字段，锁保留在真正的 owner 内；兼容 facade 调用默认实例的方法。不要在旧代码还会重新赋值包级 map 时直接改成浅拷贝别名，否则新旧 registry 会分裂。此项首先是维护性优化，尚无证据声称其性能收益达到某个比例。

构造 API 同样应在迁移后收敛：保留一个主要显式构造入口，使用领域已有的 typed deps，缺失必要依赖在构造阶段报错。对确实独立可用的小对象保留私有默认 state；不要让完整 runner 因漏传字段而静默创建一套不被 Runtime 生命周期管理的状态。

### 4.3 Lifecycle 先统一所有权，再减少状态机

目前 stop、禁止新 callback、join、最后 reset 的顺序是必要的。回调内 Stop/Close 的重入处理也已有专门测试，不建议为“优美”直接删除同步机制。

建议给每个组件明确一个 owner：谁创建、谁 Stop、谁 Join、谁释放。Runtime 负责任务组件；Process 负责跨任务可共享资源。业务 callback 请求 Stop，拥有者完成 Join/释放。只有证明调用契约收窄以后，才能删除重复关闭注册和兼容状态机。

### 4.4 性能优化先看真实热点

本次 `BenchmarkPriceStateWrite` 为 **32 B/op、2 allocs/op**，对应 `com/price_state.go:141`、`:145` 每次发布两个价格对象。父提交也存在价格对象分配，不能据此声称本次新增了回退。

可评估把价格快照存为具体值，在锁内拷贝读取，从而避免每次写入堆分配。必须先验证交易对与基础币别名的更新语义；不可直接原地修改旧指针，因为当前部分读取在释放锁后访问对象，原地修改会引入 race。若 profiler 显示收益有限，则维持现状。

不要为了去掉少量分支引入 unsafe service locator、goroutine-local、通用依赖图或 typed OHLCV 快速路径。优先减少业务循环中反复归一化配置、重复构造绑定和不必要的分配。

## 5. 拟执行的最小迁移顺序

| 阶段 | 改动范围 | 完成标准 |
| --- | --- | --- |
| 1. 可重现基线与确切缺陷修复 | go.mod/banexg 版本、Process 释放、QuestDB intent 保留、文档、固定测试配置和行情 | 干净 checkout 可构建，资源与恢复专项回归通过，单任务结果可重现 |
| 2. 存储绑定 | orm 连接/后端 owner、symbol 和 series 持久化入口 | 同库协调不丢失，异库隔离成立，不再借全局后端选择 SQL |
| 3. 完整 runner 依赖闭合 | entry、live、opt、RPC/Web/后台服务的剩余全局访问 | 显式 runner 全链路不安装全局变量、不要求 legacy session |
| 4. 生命周期收口 | Process/Runtime 与服务的 Stop/Join/Close 责任 | 启动失败、正常结束、回调内取消、并发关闭均无泄漏或互相关闭 |
| 5. 删除兼容复杂度 | 旧可变 globals、Backup/Restore、重复构造变体、State 的指针容器 | 调用方迁移完成后删除 gate 依赖；每种状态只有一个实际 owner |
| 6. 验收与定点优化 | 双 runner 场景、race、确定性对比、benchmark/profile | 并发运行与隔离结果成立，性能比较有一致输入和版本依据 |

阶段可以切成小提交；每次闭合一条调用链，不一次性重排整个包结构。清理前先补缺失的行为回归测试。本轮已完成阶段 1、2、4 的主要修复、阶段 3 的 typed runner 接线及确定性局部并发验收；阶段 3 的真实数据库/生产服务演练和阶段 5 的兼容层删除仍属于后续迁移与环境验收，不是本轮 High/Medium 实现缺口。

## 6. 必要验收矩阵

- **任务隔离：** `opt` 回测 A/回测 B、`live` 模拟实盘 A/模拟实盘 B 已同时运行；同账户名、同交易对但不同配置/价格/时钟，结果与各自串行基线一致。真实数据库 runner 仍需环境验收。
- **取消隔离：** `opt`/`live` 已覆盖 A 在 callback 内取消且 B 继续收到行情，并验证 Process/Runtime 生命周期；真实 socket、数据库和生产 worker 的无泄漏验收仍需环境演练。
- **存储隔离/共享：** 不同数据库与不同后端不可串库；同库 SID、WAL 和表锁协调仍有效。
- **任意时序数据：** 默认字段与扩展列统一经过 Values；聚合、复权、DataHub、feeder、回测与实盘 callback 保留字段类型、缺失值和显式 NULL。
- **QuestDB 安全：** 延迟可见、硬错误、超时、rename 失败、替换前验证失败时不误删旧表、不丢恢复标记。不能以删掉 WAL 等待换取“简化”。
- **性能：** 同机器、固定 Go/banexg/策略/行情/配置、非 race 模式做父提交与候选版本多次比较；记录耗时、allocs、峰值内存及订单结果。微基准零分配不替代整条 runner 验证。

## 7. 本次验证

环境：Go `1.24.13`，Linux amd64，Intel Xeon Processor (Skylake, IBRS)；`github.com/banbox/banexg v0.2.64` 由 Go proxy 解析，工作树没有本机绝对路径 `replace`。

- `go test ./runtime ./biz ./data ./strat ./live ./opt ./com ./entry ./orm/ormo -count=1`：通过；新增的 `TestRuntimeSeriesSourcesAndKlineFieldsStayOwned` 也通过。
- `go test ./... -run '^$' -count=0`：通过，所有包完成编译。
- `go vet ./...`、`go build ./...`、`go mod download`、`go list -m github.com/banbox/banexg`：均通过，依赖版本为 `v0.2.64`。
- `go test -race ./runtime ./biz ./data ./strat ./live ./opt ./orm/ormo -count=1`：通过；包含 `TestBacktestRuntimeRunnersConcurrentMatchSerialBaseline`、`TestBacktestRuntimeCancellationIsolatedFromSibling`、`TestLiveRuntimeRunnersConcurrentMatchSerialBaseline` 和 `TestLiveRuntimeCancellationIsolatedFromSibling` 等确定性双 runner 验收。
- ORM 较宽专项筛选 `-run 'Test(Quest|SymbolState|Series|AddSymbols|SID)' -timeout=120s` 达到整包时间上限；超时输出显示当时运行 `TestAddSymbolsSameIdentityAcrossRecoveryRootsReusesSharedSID` 仅 0 秒，不能据此认定这个测试死锁。
- 随后明确选取六个 QuestDB/SID 用例单独执行通过：`TestSymbolStateInstancesAreIndependent`、`TestWaitForQuestKlineCoverageVisibleTimeoutIsRetryable`、`TestWaitForQuestSeriesCoverageVisibleTimesOut`、`TestBuildQuestCompactRewriteSQLPreservesDynamicColumnsAndNullMarker`、`TestReplaceVerifiedCompactTableRestoresBeforeDroppingInvalidActivation`、`TestReplaceVerifiedCompactTableRejectsCorruptionAfterAnyRow`。覆盖实例隔离、可见性超时和删除前验证的代表路径，不能替代依赖外部行情/数据库 fixture 的 ORM 全量测试。

代表性 benchmark（一次抽测，与其他验证同时执行，时间数值仅供定位，不能用于父提交性能回归判断）：

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SymbolParserCacheHit | 8.953 | 0 | 0 |
| SymbolParserCacheHitAlternating | 9.264 | 0 | 0 |
| PriceStateRead | 74.37 | 0 | 0 |
| PriceStateWrite | 273.7 | 32 | 2 |
| RuntimeClock | 12.85 | 0 | 0 |
| BatchStateTakeReadyNonDeterministic | 133.4 | 0 | 0 |
| BatchStateTakeReadyDeterministic | 102.8 | 0 | 0 |

命令：`go test ./core ./com ./runtime ./strat -run '^$' -bench 'Benchmark(SymbolParserCacheHit|PriceStateRead|PriceStateWrite|RuntimeClock|BatchStateTakeReady)' -benchmem -count=1`。

本次没有生产交易验证、跨主机 SID 故障演练、真实数据库 runner 端到端验收或同输入父提交端到端性能对比；这些属于环境验收。剩余验收应按第 6 节执行，不能用旧文档的历史结果替代。
