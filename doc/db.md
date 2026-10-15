# 数据库双后端与任意时序数据（当前实现）

本文按当前源码说明 QuestDB、TimescaleDB 和通用时序数据，不再将已完成的支持列为待实施迁移。部署配置见[数据库指南](../bandoc/zh-CN/guide/database.md)，数据接入见[时序使用指南](series_usage.md)。

## 1. 存储职责

| 领域 | 存储与接口 |
| --- | --- |
| K 线、自定义时序、符号、日历、复权和覆盖元数据 | QuestDB 或 TimescaleDB；orm.Storage / Queries / SeriesRepo |
| TS 订单、钱包与任务状态 | ormo 的 SQLite 与任务状态 |
| UI 任务 | ormu 的 SQLite |
| 多策略共享执行 | execution Store/MemoryStore/SQL ledger；不属于时序表 |

`orm.OpenStorage(ctx, databaseConfig, dataDir)` 建立显式连接依赖；创建该资源的入口负责 Close，Runtime 只借用。SymbolState 管理所属符号索引和 SID 分配，身份由 exchange+market+symbol 决定，exg_real 是来源元数据。包级 Setup/Conn 是旧调用链入口，不是跨 Runtime 的状态访问方式。

## 2. 配置和 schema

`database.db_type` 接受 questdb 或 timescale。未设置时 8812/5432 分别识别为 QuestDB/PostgreSQL，其他端口探测；建议显式配置。QuestDB 本地默认端口可走自动安装启动流程；TimescaleDB 服务和扩展需预先部署。auto_create 初始化/升级 banbot schema，不创建 PostgreSQL 服务、账号或数据库。

```yaml
database:
  db_type: timescale
  url: postgresql://banbot:your-password@127.0.0.1:5432/banbot?sslmode=disable
  max_pool_size: 50
  auto_create: true
```

schema 和迁移实现位于 orm/sql/pg_schema.sql、pg_schema2.sql、pg_migrations.sql、qdb_migrations.sql 及后端初始化代码。业务通过显式 Storage 或对应仓储访问，不能按交易所实现自己的数据库或接入分支。

## 3. 统一数据模型与列

v0.3.8 的固定 K 线模型已扩展为任意时序读写。`DataRecord.Values map[string]any` 保存持久行字段，`DataSeries.Values map[string]any` 是 feeder、DataHub、聚合、复权、回测与实盘回调的统一传输合同；不引入 typed OHLCV 快速路径。

`NewSeriesInfo(name,timeframe,fields)` 默认生成 name_timeframe 表与 ts/end_ms/sid 列。时间为毫秒 `[TimeMS,EndMS)`，要求 EndMS>TimeMS。字段类型映射：

| 逻辑类型 | TimescaleDB | QuestDB |
| --- | --- | --- |
| float | DOUBLE PRECISION | DOUBLE |
| int | BIGINT | LONG |
| string | TEXT | STRING |
| bool | BOOLEAN | BOOLEAN |
| json | JSONB | STRING |

运行时 map 保留具体 Go 类型、显式 NULL 和缺键；数据库只存 schema 列，int/float 会规范化，JSON 按后端编码，固定列往返不能恢复原始缺键或所有 Go 类型。不能宣称数据库/JSON 网络往返等于 map 深复制。

K 线默认列包含 open/high/low/close/volume/quote/buy_volume/trade_num，已无市场特定 info 字段。TimescaleDB K 线使用 time 毫秒列，QuestDB 使用 ts TIMESTAMP。`NewKLineSeriesInfo` 配合 `KLineSeriesStore` 为已有行写扩展列，缺少对应 K 线时失败；独立 SeriesStore 写完整独立序列。`Queries.GetSeriesFields/QuerySeriesFields` 读取字段投影，旧 GetOHLCV 只提供兼容 K 线视图。

## 4. 覆盖范围和补齐

两端复用 sranges，按 `(sid,table,timeframe)` 描述已回答区间，has_data 区分有数据与确认无数据。Missing/FillMissing/UpdateCoverage 处理区间和空洞，不能仅用 min/max 推断中间连续。source 的 FetchHistory 必须区分真实空结果与失败，不能把抓取失败记录为确认无数据。

SeriesStore 统一处理 sid=0 补齐、不同 sid 拒绝、写入与覆盖更新。TS SeriesRuntime/HistSeriesFeeder 和双引擎 SubscriptionPlan 复用这些数据能力；计划合并字段与最大 warmup，事件计数预热必须基于实际观测。回放还需遵守所属任务的历史覆盖契约，不能绕过限定读路径隐式重下载。

## 5. 聚合、复权与传输

ExSymbol.AggRules 保存列级 JSON 规则，RegisterAggRule 可扩展 first/last/min/max/sum/avg/mid。默认 OHLCV 使用其语义，扩展列默认 last。first/last 保留选中原始值；数值聚合依规则转换并校验 NULL/缺字段，不代表任意输入完全原样保留。

feeder 复权复制 Values，并调整 open/high/low/close/volume/buy_volume；自定义字段、quote 和 trade_num 不自动乘价格倍率。`SeriesOHLCV/AsKline` 是局部兼容视图，不能用它们替换通用字段传输。

Spider/Watcher 消息使用 NotifySeries/SeriesMsg 的 Rows DataSeries；BanIO payload 是 JSON，需遵守类型恢复边界。异步启动缓冲使用递归复制，保留具体类型与 NULL。普通数据库最新值不能证明严格 PIT；因子严格历史要求事件/可见/接收时间、修订和版本证据，static-approximation 应明确声明。

## 6. QuestDB WAL 与安全表替换

QuestDB WAL 是异步写后读模型，成功 INSERT 或 CREATE TABLE AS 不等于紧随其后的查询可见。依赖写入结果时先等待预期行、时间戳、范围或记录数。Metadata 同进程写后读优先采用定向可见性等待、缓存或锁，不靠盲目重试或首次空读回退。

自定义序列删除先将目标覆盖范围标记为无有效数据，读取按有效 sranges 过滤。当前删除命中比例达到 50% 才整理物理表。TimescaleDB 可事务内物理删除；QuestDB 重写必须验证替换表与预期快照相符，之后才 DROP/RENAME。写前后快照、恢复标记与锁由相关仓储维护；超时保留标记，不能因为第一读为空清除恢复状态或删除旧表。

相关实现为 orm/questdb_visibility.go、questdb_visibility_test.go、series_repo.go、srange.go、exsymbol_recovery.go。修改可见性/替换逻辑时必须有超时/轮询或删除前验证专项回归。

## 7. 数据迁移与验证

两端数据文件和物理 schema 不能直接互换。应先备份，再导出、核对字段/时间/NULL/覆盖范围、转换并导入新环境，确认新快照后切换。旧 OHLCV CSV/protobuf 工具不等于任意扩展字段迁移器，使用前核实导出 schema；自定义源需完整导出其 SeriesInfo 和值。不要直接执行旧方案中的 info 字段迁移 SQL 或未经快照验证的整表替换。

普通回归可先运行 SeriesStore/SeriesAccess/Series/Quest 可见性相关测试；真实后端验证需单独部署数据库与配置，并显式启用 BANBOT_TEST_INTEGRATION。文档更新不是已执行生产迁移或实机 WAL 验证的证据。
