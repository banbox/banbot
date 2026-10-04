# data 包

`data` 负责历史回放、实时采集、订阅需求与数据源生命周期。Provider、Feeder 和 Watcher 统一传递 `orm.DataSeries`；默认 K 线和扩展字段都在 `Values map[string]any` 中。`FnDataSeries` 与 `FuncEnvEnd` 均为 `func(*orm.DataSeries)`。

## 通用时序数据

### DataSource 与 DataSink

```go
type DataSink interface {
    Emit(sub *orm.Subscription, rows []*orm.DataRecord) error
}
type DataSource interface {
    Info() *orm.SeriesInfo
    FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error)
    SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error
}
```

`strat.DataSub` 是 `orm.Subscription` 的 alias，旧函数式 source 签名仍可使用它。`FetchHistory` 按 `[startMS,endMS)` 返回有真实时间区间的记录；不应伪造数据来覆盖未查询的区间。实时推送用 `sink.Emit`，省略实时函数不会自动轮询。

### DataSourceCatalog

`NewDataSourceCatalog()` 创建独立注册表。`RegisterDataSource` 注册实例；`RegisterDataSourceFactory(name, factory)` 注册创建独立实例的工厂；`RegisterFuncDataSource(info, fetch, subscribe)` 自动注册函数式工厂。每个 catalog 的 source 名称唯一。

显式 Runtime 使用 `RuntimeCatalogFromRegisteredSources()` 从进程注册定义创建自己的实例和状态；只经包级 `RegisterDataSource` 注册且没有工厂的源会被拒绝。复杂源应注册工厂，每次返回独立 source，避免关闭一个任务时影响另一个任务。

`GetDataSource`、`ListDataSources`、`ListDataSourceStatus` 有 catalog 方法和包级兼容入口。包级入口不是另一个 Runtime 的状态访问器。

### SeriesRuntime / SeriesPlan

传统 TS 任务使用 `NewSeriesRuntimeWithRuntimeDeps(deps, sink)`、`Plan/Ensure/Apply/ActivateNew`，从 job 收集非主 K 线订阅，补齐覆盖范围并激活新增 source。`HistSeriesFeeder` 将独立历史序列与 K 线按可见时间一起回放。`ThirdPartySeriesBootstrap` 是 `SeriesPlan` 的兼容 alias。

### SubscriptionPlan

双引擎通过 `catalog.CompileSubscriptionPlan(ctx, requests, options)` 合并 `(source,sid,timeframe)` 需求，保留 consumer 的 required、新鲜度与 warmup，合并字段投影并取最大预热量。

```go
type SubscriptionPlanOptions struct {
    Namespace string
    AnchorMS, EndMS int64
    PageRows, PrefetchRows int
    PageBytes int64
    RequireManagedLive bool
}
```

`Streams/Subscriptions/SourceMetadata/BudgetReport` 返回计划视图；`Bootstrap(ctx, repo)` 补齐历史。PageBytes 限制解码输入页，不是进程堆内存上限。事件流声明使用 `FrequencyEvent`、`TimeFrame: "event"`；计数预热需要 source 实现 `ObservationWarmupSource.WarmupStart`，不能将条数乘一个虚构周期。

### 实盘安装

`PrepareLivePlan` 准备独立订阅、预热和启动缓冲；`SubscriptionInstallation.Activate` 激活，`CommitPrepared` 提供提交回调边界；`InstallLivePlan` 是准备并激活的入口。需要受管理的实盘源必须实现 `ManagedLiveSource.SubscribeManaged`，返回具有 `Stop()` 和 `Join() error` 的独立句柄；可用 `LiveSourceErrors` 报告异步失败。

启动队列递归复制 Values，保留具体类型、typed nil、NULL、缺失键、时间信息和复权元数据。达到预算、源失败或取消会报错。`Stop` 先封闭接收，所属 owner 再调用 `Join` 等待已接纳回调和生产者；不要在回调中等待自己退出。候选代准备失败不能关闭仍在服务的旧代。

## Provider 和 Feeder

| 当前类型 | 职责 |
| --- | --- |
| `Feeder` / `SeriesFeeder` | 一个标的的多周期输入、预热、聚合与回调 |
| `DBSeriesFeeder` | 数据库历史批次回放；`GetBatch/RunBatch/CallNext` 推进 |
| `TfSeriesLoader` | 单标的单周期数据读取与 seek |
| `HistSeriesFeeder` | 独立 source 的历史回放 |
| `HistProvider` | 管理历史 feeder，按结束时间推进 |
| `LiveProvider` | 管理实时 feeder 与 `SeriesWatcher` |

接口为 `IDataFeeder`、`IHistFeeder` 和 `IHistDataFeeder`，不是旧的 IKlineFeeder/IHistKlineFeeder。主要构造器：

```go
NewSeriesFeeder(exs *orm.ExSymbol, callback FnDataSeries, showLog bool) (*SeriesFeeder, *errs.Error)
NewDBSeriesFeeder(exs *orm.ExSymbol, callback FnDataSeries, showLog bool) (*DBSeriesFeeder, *errs.Error)
NewHistProvider(callback FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, progress *utils.StagedPrg) *HistProvider
NewLiveProvider(callback FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error)
```

这些类型提供 `WithRuntimeDeps` 构造入口；显式任务应提供 Runtime 的配置、符号、存储、时钟、策略与回调边界。Provider 支持 `SubWarmPairs/UnSubPairs/LoopMain`，实盘资源支持 `Stop/Join`。公开回调 `Feeder.CallBack`、`Feeder.OnEnvEnd` 的输入均是 DataSeries，待处理值是 `WaitData`。

内置 K 线读取最小物理周期，再聚合派生周期；投影合并后扩展列经相同 Values 传递，存储补充只补缺键，显式 NULL 不被覆盖。聚合按字段规则执行；复权只修改框架支持的字段，自定义字段不被丢弃。分别需要不同起止范围时，可单独创建 Provider 或使用 SeriesStore/Queries.GetSeriesFields 等存取接口。

## Spider 与 Watcher

`LiveSpider` 管理每个交易所/市场的 `Miner`，负责采集、存储与广播。当前消息为 `NotifySeries{TFSecs, Interval, Rows []*orm.DataSeries}`；`SeriesMsg` 嵌入它并增加 `ExgName/Market/Pair`。存储任务是 `SaveSeries`。旧 NotifyKLines/KLineMsg/SaveKline 文档字段已由这些结构替代。

`NewSeriesWatcherWithRuntimeDeps(deps, addr)` 创建所属任务的 TCP 客户端，`OnDataMsg func(*SeriesMsg)` 接收序列。`WatchJobs` 声明交易所、市场、类型和标的周期；`UnWatchJobs` 取消。Spider/Watcher 通过 BanIO 发送完整 Rows；网络 payload 使用 JSON，不能据此保证 map 内整数宽度或自定义 Go 类型自动往返，严格类型源必须有 schema 和解码校验。

## 数据工具

`FindPathNames`、`ReadZipCSVs` 处理文件，`RunFormatTick/Build1mWithTicks` 处理逐笔数据，`CalcFilePerfs` 分析文件。普通数据读取与数据源注册见[自定义时序数据](../guide/custom_data.md)，多因子原生图和回测/实盘组合见[因子 API](factor.md)。
