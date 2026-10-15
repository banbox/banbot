# 数据库

banbot 的行情、时序数据和相关元数据可使用 QuestDB 或 TimescaleDB 存储。两者都通过 PostgreSQL 协议连接，策略、回测、K 线工具和自定义时序数据的使用方式相同；只需在配置中选择一个后端。

## 选择后端

| 后端 | 适用场景 | 部署方式 |
| --- | --- | --- |
| QuestDB | 本地快速开始、以行情写入和读取为主 | banbot 在本地默认端口连接失败时可自动下载安装并启动 |
| TimescaleDB | 已有 PostgreSQL 基础设施、希望由运维统一管理数据库 | 请先自行部署 PostgreSQL 和 TimescaleDB 扩展 |

不要让同一个 `BanDataDir` 下的不同机器人以不同后端交替运行。两种后端的表结构和数据文件不能直接互换；迁移前请单独规划数据导出、转换和导入流程。

## QuestDB 配置

QuestDB 的默认 PGWire 端口为 `8812`。配置中明确指定 `db_type`，可避免端口变化时识别错误。

```yaml
database:
  db_type: questdb
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
  max_pool_size: 50
  auto_create: true
  qdb_mem_pct: 0.3       # 可用内存的使用比例，范围 0~1
  qdb_max_mem_mb: 16384  # 内存使用上限，单位 MB
```

本机没有可连接的 QuestDB 时，banbot 会尝试安装并启动它。`qdb_mem_pct` 和 `qdb_max_mem_mb` 只对这一后端生效。QuestDB 的 WAL 写入是异步可见的：框架相关写后读路径使用定向可见性等待或进程内缓存/锁；成功 INSERT 或 CTAS 不等于随后查询立刻可见。依赖写入结果时，应等待预期行、时间戳、范围或记录数，而非仅看一次空查询。DROP/RENAME 前必须验证替换表符合预期快照；等待超时必须保留恢复标记，不得把空读解释为无需恢复。外部 SQL 工具也需要遵守这些规则。

## TimescaleDB 配置

TimescaleDB 必须由您先部署，并确保目标数据库已安装 TimescaleDB 扩展。banbot 不会下载或启动 PostgreSQL/TimescaleDB。默认 PostgreSQL 端口为 `5432`。

```yaml
database:
  db_type: timescale
  url: postgresql://banbot:your-password@127.0.0.1:5432/banbot?sslmode=disable
  max_pool_size: 50
  auto_create: true
```

`auto_create: true` 会在连接成功后初始化或升级 banbot 所需的 schema，不会创建 PostgreSQL 服务、用户或数据库。生产环境请为 banbot 使用单独的数据库账号，并按您的部署要求设置 TLS、密码和网络访问控制。

## 自动识别

未设置 `db_type` 时，banbot 会根据连接端口识别：`8812` 视为 QuestDB，`5432` 视为 PostgreSQL/TimescaleDB；其他端口会继续探测。新配置建议始终设置 `db_type`，使行为可读且稳定。

## 时序数据语义

无论选用哪个后端，banbot 都通过统一的时序接口管理 K 线和自定义数据，包括建表、写入、读取、覆盖范围和缺口回填。K 线覆盖范围由内部 `sranges` 机制维护，不应依赖历史版本中的 `KInfo`、`KHole` 等表作为外部集成契约。

时序字段按 schema 写入固定列，支持 float/int/string/bool/json；默认 K 线和扩展字段统一通过 DataSeries.Values 传递。数据库整数为 64 位，JSON 的数据库编码与运行时任意 Go 类型不是同一合同；缺键与显式 NULL 的差别不能在固定列往返后恢复。

QuestDB 自定义序列删除先更新 sranges 的有效覆盖范围，达到当前整理阈值后才重写物理表并验证替换快照；TimescaleDB 使用事务和物理删除。不要绕过仓储直接修改覆盖元数据。ormo 订单状态与 ormu UI 任务使用独立 SQLite，共享执行账户的 ledger/store 也有自己的持久化职责，不能混同为时序表。

自定义时序数据的接入、存储和策略消费请参阅[自定义时序数据](./custom_data.md)。
