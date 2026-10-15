# 数据处理与数据源

[English API](../bandoc/en-US/api/data.md) / [中文 API](../bandoc/zh-CN/api/data.md)

data 管理数据注册、订阅需求、历史回放和实盘采集。主链路使用 orm.DataSeries，默认 K 线和扩展字段统一放在 Values map[string]any；SeriesOHLCV 是兼容视图，不替代通用传输。

## 接入与任务隔离

简单源使用 RegisterFuncDataSource(info,fetch,subscribe)，复杂源使用 RegisterDataSourceFactory，每个显式 Runtime 创建独立 DataSourceCatalog/source/status。单独注册共享实例不能满足 Runtime 工厂合同。历史行用 DataRecord，实时批次经 DataSink.Emit，订阅由 orm.Subscription（strat.DataSub alias）声明。

SeriesRuntime/HistSeriesFeeder 支持 TS 自定义源；双引擎 SubscriptionPlan 合并字段、最大 warmup、consumer 必需性和新鲜度，支持 bar 与 event 声明。event计数预热需 ObservationWarmupSource 基于真实观测确定起点。合法声明不是 reader 支持的证明。

## Provider、字段与生命周期

当前类型是 SeriesFeeder、DBSeriesFeeder、TfSeriesLoader、HistProvider、LiveProvider 和 SeriesWatcher，回调均传 DataSeries。聚合使用列规则，复权保留非调整字段；扩展列补充不会覆盖显式 NULL。startup 队列递归复制 Values、具体类型、typed nil、时间和复权元数据，预算限制不能靠丢弃字段满足。

实盘受管理 source 返回独立 Stop/Join 句柄。Prepare/Warmup/Commit 候选代失败保留旧代；owner Stop封闭输入后 Join等待接纳回调，再释放所属资源。Spider消息是 NotifySeries/SeriesMsg.Rows；BanIO payload 是 JSON，不能将其视为任意 Go 类型的无损复制。

strict PIT 需要版本与可见性证据，数据库latest-value必须明确 static-approximation。见[时序指南](../doc/series_usage.md)、[多因子指南](../bandoc/zh-CN/guide/factor.md)、[BanIO](../doc/banio.md)与[审查记录](../doc/strategy_engine_refactor.md)。
