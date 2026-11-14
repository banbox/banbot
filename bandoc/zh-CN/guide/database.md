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
  qdb_mem_pct: 0.3       # 系统总内存的分配比例，有效范围 (0,1]
  qdb_max_mem_mb: 16384  # 内存使用上限，单位 MB
```

连接失败且后端被判定为 QuestDB 时，banbot 会尝试在运行机器上安装并启动 QuestDB；它不会替您部署远程服务器。自动安装支持 macOS、Linux、Windows，首次安装需要网络；离线使用请预先安装数据库。自动启动的数据目录为 `<BanDataDir>/qdbdata`。远程连接失败时应先检查目标服务、地址、端口和认证，不要把本机自动启动当作远端恢复。

`qdb_mem_pct` 默认 0.3，`qdb_max_mem_mb` 默认 16384，仅在 banbot 启动本机 QuestDB 时用于内存调优，不是已有远程服务或整个进程的硬内存限制。比例基于系统总内存，0 或无效值采用默认值。

QuestDB WAL 写入异步可见，成功写入不保证下一次查询立即读到数据。手动 SQL 维护时应等待预期数据可见，替换表前核实新表内容，超时保留恢复标记；不要因一次空查询删除旧表或清理恢复状态。

## TimescaleDB 配置

TimescaleDB 必须由您先部署，并确保目标数据库已安装 TimescaleDB 扩展。banbot 不会下载或启动 PostgreSQL/TimescaleDB。默认 PostgreSQL 端口为 `5432`。

```yaml
database:
  db_type: timescale
  url: postgresql://banbot:your-password@127.0.0.1:5432/banbot?sslmode=disable
  max_pool_size: 50
  auto_create: true
```

`auto_create: true` 时，需要连接行情数据库的命令（如 `kline down`、`backtest`、`trade`、`web`）在连接成功后都会初始化或升级 banbot 所需的 schema（表、hypertable 和迁移版本），该过程幂等，可重复执行；它不会创建 PostgreSQL 服务、用户或数据库。若设为 `false`，请确保 schema 已由其他启用 `auto_create` 的实例初始化，否则会出现 `relation "exsymbol" does not exist` 等错误。生产环境请为 banbot 使用单独的数据库账号，并按您的部署要求设置 TLS、密码和网络访问控制。

### 部署 TimescaleDB

无论使用哪种安装方式，都需要满足：

1. `postgresql.conf` 中 `shared_preload_libraries` 包含 `timescaledb`，修改后重启 PostgreSQL；
2. 为 banbot 创建用户和数据库；
3. 由超级用户在目标数据库中执行 `CREATE EXTENSION IF NOT EXISTS timescaledb;`。banbot 初始化 schema 时也会执行此语句，但普通账号通常没有创建该扩展的权限，提前创建可避免初始化失败。

以下 Docker 示例将数据持久化到 `/opt/tsdb`；镜像版本应与您选择的 PostgreSQL/TimescaleDB 版本匹配。macOS 原生安装时，按安装输出确认 PostgreSQL 服务名与配置路径，不要直接套用其他机器的版本号或 Homebrew 路径。

```shell
docker run -d --name timescaledb -p 5432:5432 \
  -e POSTGRES_PASSWORD=your-password \
  -v /opt/tsdb:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg17
```

然后创建用户、数据库和扩展（Docker 方式请通过 `docker exec -it timescaledb psql -U postgres` 执行）：

```sql
CREATE ROLE banbot LOGIN PASSWORD 'your-password';
CREATE DATABASE banbot OWNER banbot;
\c banbot
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

`bot init` 生成的 `config.yml` 默认使用 QuestDB。使用 TimescaleDB 时，请在 `config.local.yml` 中按上文配置 `database`，覆盖默认值。

## 连接池与多进程写入

未设置 `max_pool_size` 时默认取 `max(40, CPU 核数 × 4)`。低于 30 会产生连接池告警，大批量下载时可能耗尽连接。

多个进程共同向 QuestDB 注册新标的时，可配置 `database.sid_registry_url` 指向一个独立 PostgreSQL SID 注册库，例如 `postgresql://banbot:your-password@127.0.0.1:5432/sid_registry?sslmode=disable`。相关进程应共享同一注册库；请先创建服务、用户和数据库，`auto_create` 仅负责注册表 schema。它不存储行情，也不把 QuestDB 切换成 TimescaleDB。标的身份由 exchange、market、symbol 决定，`exg_real` 只是元数据。

## 自动识别

未设置 `db_type` 时，banbot 会根据连接端口识别：`8812` 视为 QuestDB，`5432` 视为 PostgreSQL/TimescaleDB；其他端口会继续探测。新配置建议始终设置 `db_type`，使行为可读且稳定。

## 时序数据语义

无论选用哪个后端，banbot 都通过统一的时序接口管理 K 线和自定义数据，包括建表、写入、读取、覆盖范围和缺口回填。K 线覆盖范围由内部 `sranges` 机制维护，不应依赖历史版本中的 `KInfo`、`KHole` 等表作为外部集成契约。

时序字段按 schema 写入固定列，支持 float/int/string/bool/json；默认 K 线和扩展字段统一通过 DataSeries.Values 传递。数据库整数为 64 位，JSON 的数据库编码与运行时任意 Go 类型不是同一合同；缺键与显式 NULL 的差别不能在固定列往返后恢复。

删除、回填和覆盖范围更新应通过数据工具或 `SeriesStore` 完成，不要直接改写覆盖元数据。订单状态和 UI 任务保存在独立 SQLite 文件中，不受上述行情数据库后端选择影响。

自定义时序数据的接入、存储和策略消费请参阅[自定义时序数据](./custom_data.md)。
