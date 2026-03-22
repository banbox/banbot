# 数据库双后端支持技术实施方案

## 背景与目标

banbot 从 v0.2.x（banbotraw）迁移至 v0.3.7 时，将时序 K 线存储从 **TimescaleDB**（PostgreSQL 超表扩展）切换到了 **QuestDB**（PGWire 协议）。现在需要在 v0.3.7 基础上**恢复对 TimescaleDB 的支持**，同时保留对 QuestDB 的完整支持，使用户可以在两者之间选择。

本文档分析两个版本的核心差异，并给出优雅兼容两种数据库的架构方案。

---

## 一、两个版本的核心差异分析

### 1.1 数据库拓扑对比

| 维度 | v0.2.x（TimescaleDB） | v0.3.7（QuestDB） |
|------|----------------------|-------------------|
| 时序 K 线引擎 | PostgreSQL + TimescaleDB 超表 | QuestDB PGWire |
| 关系/元数据存储 | PostgreSQL（同一实例） | QuestDB |
| K 线表主键 | `(sid, time)` UNIQUE INDEX | `DEDUP UPSERT KEYS(sid, ts)` |
| 时间字段类型 | `int8`（毫秒 Unix 戳） | `timestamp`（微秒 UTC） |
| 元数据表 | `kinfo`、`khole`、`exsymbol`、`calendars`、`adj_factors`、`ins_kline`、`kline_un` | `sranges_q`、`exsymbol_q`、`calendars_q`、`adj_factors_q`、`ins_kline_q`、`kline_un_q` |
| 插入方式 | `COPY FROM`（pgx CopyFrom） | 批量 `INSERT … VALUES (…)` |
| 空洞/覆盖范围管理 | `kinfo`（总范围）+ `khole`（空洞列表） | `sranges_q`（不连续段列表，DEDUP append-only） |
| 数据库创建/迁移 | `dbconf` 表版本控制 + 事务迁移 | `schema_migrations` 表 + 幂等非事务迁移 |
| 端口区分 | 5432（PostgreSQL 默认） | 8812（QuestDB 默认） |
| 连接池模式 | 默认 prepared statement 缓存 | 禁用 statement cache（`QueryExecModeExec`） |
| 自动安装 | 无 | 有（`questdb.go` 自动下载安装） |

### 1.2 SQL 语法关键差异

**时间字段与过滤：**
- TimescaleDB：时间列名 `time`，类型 `int8`（毫秒 Unix 戳），过滤写法 `where time >= $1 and time < $2`
- QuestDB：时间列名 `ts`，类型 `timestamp`（微秒 UTC），过滤写法 `where ts >= cast($1 as timestamp) and ts < cast($2 as timestamp)`（传参时 × 1000 转换）

**K 线表字段对比（核心差异）：**

| 字段 | TimescaleDB（banbotraw） | QuestDB（banbot v0.3.7） |
|------|------------------------|------------------------|
| 时间 | `time int8`（毫秒） | `ts timestamp`（微秒） |
| 基础 OHLCV | `open,high,low,close,volume` | `open,high,low,close,volume` |
| 扩展字段 | `info float8`（单字段，语义复用：等于 buy_volume，或持仓量等） | `quote double`（计价货币成交额）、`buy_volume double`（主动买入量）、`trade_num long`（成交笔数）|

**字段语义澄清：** banbotraw 的 `info` 字段在代码中通过 `exs.InfoBy()` 决定语义（`"sum"` 时累加，`"last"` 时取最新值），实际上对应 `buy_volume` 的角色（在期货场景下也可存持仓量）。v0.3.7 将其拆分为三个独立字段，**新方案在 TimescaleDB 中也统一使用拆分后的字段，不再保留 `info`**。

**`kline_un` 表字段对比：**

| 字段 | TimescaleDB（banbotraw） | QuestDB（banbot v0.3.7） |
|------|------------------------|------------------------|
| 时间标识 | `start_ms int8` | `ts timestamp`（= start_ms 转换） |
| 扩展字段 | `info float8` | `quote,buy_volume,trade_num`（同 kline 表） |
| 有效期 | 无 | `expire_ms long` |
| 删除方式 | 物理 DELETE/UPDATE | `is_deleted boolean` 软删除 |

**去重写入：**
- TimescaleDB：`ON CONFLICT (sid, time) DO UPDATE SET …`
- QuestDB：内置 `DEDUP UPSERT KEYS`，INSERT 即幂等，无需 ON CONFLICT 子句

**LATEST BY（QuestDB 专有）：**
- QuestDB 的 `sranges_q`、`calendars_q`、`adj_factors_q` 等元数据表通过 `LATEST BY … WHERE is_deleted = false` 实现逻辑软删除。TimescaleDB 版本则直接使用 `DELETE` 和 `UPDATE`。

**批量插入：**
- TimescaleDB：使用 pgx `CopyFrom`（高吞吐）
- QuestDB：使用 `INSERT INTO … VALUES (…),(…)` 分批提交（QuestDB 不支持 CopyFrom 协议）

### 1.3 覆盖范围管理差异（重点）

这是两个版本最核心的架构差异：

**TimescaleDB 原始方案（kinfo + khole，v0.2.x）：**
- `kinfo` 存储每个 `(sid, timeframe)` 的已下载总范围 `[start, stop)`，单行记录
- `khole` 存储已下载范围内部的"空洞"段（缺失数据）
- 下载判断：只跳过已完全覆盖的外部范围（`oldStart <= startMS && endMS <= oldEnd`），无法感知内部空洞，粒度粗
- 依赖数据库事务保证 kinfo + khole 一致性
- `BulkDownOHLCV` 先批量 `GetKlineRanges` 预取所有 sid 的 `(start, stop)`，再逐个下载外部缺口

**QuestDB 方案（sranges，v0.3.7）：**
- `sranges_q` 存储每个 `(sid, tbl, timeframe)` 的所有已覆盖段（非连续允许）
- 通过 `has_data` 字段区分"有数据段"与"无数据段（空洞）"，两类信息合一
- append-only + 软删除（`is_deleted=true`）以适应 QuestDB WAL 不可 UPDATE 的限制
- 进程内 `srangesCache`（读写锁 map）补偿 QuestDB WAL commit 延迟，避免误重下
- 下载判断：`getCoveredRanges` 加载已覆盖的 has_data 段，`subtractMSRanges` 精确计算缺失段，粒度细
- `BulkDownOHLCV` 不再预取 kRanges，每个 sid 在 `downOHLCV2DBRange` 内部独立调用 `getCoveredRanges`

**新方案决策：恢复 TimescaleDB 时以 sranges 替代 kinfo+khole**

v0.3.7 的 sranges 模型在表达能力和下载精度上均优于 kinfo+khole（非连续覆盖、空洞内联、细粒度补下），且 v0.3.7 的全部业务代码都已围绕 sranges 接口构建。因此，恢复 TimescaleDB 支持时 **不恢复 kinfo/khole 表**，而是在 PostgreSQL 中建立语义等价的 `sranges` 表，实现 PG 原生版本的 sranges 操作，使两套后端共用同一套 sranges 抽象层。

PostgreSQL `sranges` 表设计与 `sranges_q` 的主要差异：

| 维度 | sranges_q（QuestDB） | sranges（PostgreSQL） |
|------|---------------------|-----------------------|
| 删除方式 | append-only + is_deleted 软删除 | 真实 DELETE / UPDATE |
| 写入原子性 | 非事务，依赖 WAL 最终一致 | 支持 PostgreSQL 事务 |
| 时间戳列 `ts` | 必须（append-only 主键） | 不需要 |
| UNIQUE 约束 | 无（每次插入均追加） | `(sid, tbl, timeframe, start_ms)` |
| LATEST BY | QuestDB 专有语法 | 不需要（无重复行） |
| 进程内缓存 | 必须（补偿 WAL 延迟） | 不需要（事务即时可见） |

### 1.4 未完成 K 线（kline_un）差异

- TimescaleDB（banbotraw）：`kline_un` 是独立 PostgreSQL 表，字段为 `(sid, start_ms, stop_ms, timeframe, open, high, low, close, volume, info)`，通过 `UPDATE … WHERE sid=? AND timeframe=?` 或 `INSERT` 实现 upsert，通过 `DELETE` 物理删除
- QuestDB（v0.3.7）：`kline_un_q` 是独立 QuestDB 表，字段拆分为 `(sid, timeframe, ts, stop_ms, expire_ms, open, high, low, close, volume, quote, buy_volume, trade_num, is_deleted, deleted_at)`，使用 `LATEST BY sid, timeframe` + `is_deleted=false` 软删除，新增 `expire_ms` 字段用于缓存有效期控制

**新方案决策：** TimescaleDB 版的 `kline_un` 表也统一采用拆分字段（`quote, buy_volume, trade_num`），去除 `info`，同时保留 `expire_ms` 字段（用于与 QuestDB 版共享 `getUnFinish` 的过期判断逻辑）。删除方式使用 PostgreSQL 原生 `DELETE/UPDATE`，无需软删除。

### 1.5 AddInsJob / DelInsKline 接口差异

- TimescaleDB：`AddInsJob` 返回 `int32`（自增 id），`DelInsKline(ctx, id)` 按 id 删除
- QuestDB：`AddInsJob` 返回 `time.Time`（时间戳），`DelInsKline(ctx, sid, tf, ts)` 按 (sid, tf, ts) 软删除

### 1.6 配置差异

`DatabaseConfig` 在 QuestDB 版新增：
- `QdbMemPct`：QuestDB 内存占比
- `QdbMaxMemMB`：QuestDB 最大内存 MB

端口检测（`pgConnPool`）：v0.3.7 中若端口为 5432 则拒绝连接（引导用户迁移至 QuestDB），需要恢复对 5432 的支持。

---

## 二、目标架构设计

### 2.1 设计原则

1. **最小侵入**：上层业务代码（`orm/kdata.go`、`orm/kline.go` 高层函数）不感知数据库类型
2. **接口隔离**：将数据库类型相关的 SQL 和操作封装在驱动层，统一暴露抽象接口
3. **运行时检测**：根据配置 URL 的端口/DSN 自动检测数据库类型，或通过配置项显式指定
4. **向后兼容**：现有 QuestDB 用户零感知变化；已有 TimescaleDB 数据可通过迁移工具导入

### 2.2 数据库类型检测策略

在 `orm/base.go` 的 `Setup()` 中，通过以下方式检测：

```
伪代码：
1. 解析 database.url 中的端口
2. 若端口 = 5432，或 database.db_type = "timescale"，则为 TimescaleDB 模式
3. 若端口 = 8812，或 database.db_type = "questdb"，则为 QuestDB 模式
4. 若未知端口，尝试执行 `select count() from tables()` 探测（QuestDB 专有语法）
   - 成功 → QuestDB
   - 失败 → 尝试 `select 1 from pg_tables limit 1` 探测
   - 成功 → TimescaleDB/PostgreSQL
```

`DatabaseConfig` 新增可选字段 `DbType`（`"questdb"` 或 `"timescale"`），优先级高于自动检测。

引入全局变量 `IsQuestDB bool`（在 `orm` 包内），Setup 完成后设定，供所有查询函数分支判断。

### 2.3 连接池差异处理

在 `pgConnPool()` 中：
- TimescaleDB：保留默认 prepared statement 缓存，移除对 5432 端口的拒绝逻辑
- QuestDB：继续禁用 statement cache、设置 `QueryExecModeExec`

```
伪代码：
if IsQuestDB:
    poolCfg.ConnConfig.DefaultQueryExecMode = QueryExecModeExec
    poolCfg.ConnConfig.StatementCacheCapacity = 0
    poolCfg.ConnConfig.DescriptionCacheCapacity = 0
```

### 2.4 Schema 初始化与迁移

**TimescaleDB 路径：**
- 嵌入 `orm/sql/pg_schema.sql`（含 **sranges**、exsymbol、calendars、adj_factors、ins_kline、kline_un；**不含** kinfo/khole）
- 嵌入 `orm/sql/pg_schema2.sql`（含 kline_1m~1d 超表）
- 嵌入 `orm/sql/pg_migrations.sql`（`schema_migrations` 表 + 幂等 DDL）
- `runMigrations` 使用 `schema_migrations` 表 + 幂等方式（与 QuestDB 保持一致，避免维护两套迁移机制）

**QuestDB 路径：**
- 保持现有 `orm/sql/qdb_migrations.sql` + `schema_migrations` 表

在 `Setup()` 中：
```
伪代码：
detectDbType()
if IsQuestDB:
    runQdbMigrations()
else:
    runPgMigrations()
    initSQLitePaths()  // SQLite 仅用于 orders/trades，TimescaleDB 自带元数据表
```

### 2.5 时间字段与 K 线列适配

K 线表的时间字段和扩展列是最核心的 SQL 差异：

**时间字段：**

| 操作 | TimescaleDB SQL | QuestDB SQL |
|------|----------------|-------------|
| 写入时间 | `time = $1`（int8 ms） | `ts = cast($1*1000 as timestamp)`（或 `time.UnixMilli(...).UTC()`） |
| 读取时间 | `select time …`（直接 ms） | `select cast(ts as long)/1000 …`（除 1000 转 ms） |
| 范围过滤 | `time >= $1 and time < $2` | `ts >= cast($1 as timestamp) and ts < cast($2 as timestamp)`（*1000） |

**K 线列（统一字段，无 `info`）：**

新方案中 TimescaleDB 的 kline 表也采用与 QuestDB 相同的列集合：`open, high, low, close, volume, quote, buy_volume, trade_num`，彻底去除 `info` 字段。这样两套后端的上层读写逻辑（`mapToKlines`、`InsertKLines`、`calcUnfinishFromSubs` 等）可以共用同一套字段映射，只需在 SQL 文本的时间列名和过滤语法上分支。

**SQL 辅助函数（在 `orm/kline.go` 中）：**

```
伪代码：
func buildTimeFilter(startMs, endMs int64) string:
    if IsQuestDB:
        return fmt.Sprintf("ts >= cast(%d as timestamp) and ts < cast(%d as timestamp)", startMs*1000, endMs*1000)
    else:
        return fmt.Sprintf("time >= %d and time < %d", startMs, endMs)

func buildTimeSelect() string:
    if IsQuestDB:
        return "cast(ts as long)/1000"
    else:
        return "time"

func buildKlineColumns() string:
    // 两套后端完全相同，无需分支
    return "open,high,low,close,volume,quote,buy_volume,trade_num"
```

**`refreshAgg`（聚合更新）的影响：** banbotraw 中 `refreshAgg` 使用 PostgreSQL 的 `GROUP BY` 内联聚合 SQL（`INSERT INTO … SELECT … GROUP BY`）来高效更新大周期表，其中 `info` 字段通过 `aggCol("info", infoBy)` 动态生成聚合表达式。新方案中：
- TimescaleDB 路径：仍可使用 `INSERT INTO … SELECT … GROUP BY` 内联聚合，但需将 `info` 替换为 `quote`（sum）、`buy_volume`（sum 或 last，取决于 `infoBy`）、`trade_num`（sum）三列的独立聚合表达式
- QuestDB 路径：保持现有的先查询再 `BuildOHLCV` 再 `InsertKLines` 的应用层聚合方式（QuestDB 不支持 `GROUP BY` 内联聚合写入）
- 两套路径在 `refreshAgg` 函数内通过 `IsQuestDB` 分支选择

### 2.6 覆盖范围管理（核心适配）

这是改动最大的部分。v0.3.7 的全部下载判断和范围更新逻辑都围绕 sranges 接口构建：`getCoveredRanges`、`UpdateSRanges`、`UpdateSRangesWithHoles`、`UpdateSRangesWithHolesPg`、`DelKInfo`、`GetKlineRange`、`GetKlineRanges`。

**方案：PostgreSQL sranges 原生实现，共用上层 sranges 抽象**

不恢复 kinfo/khole，而是在 PostgreSQL 中建立 `sranges` 表并实现对应的操作函数。两套后端共享相同的业务逻辑层，只在底层 SQL 实现上分支。

**PostgreSQL `sranges` 表结构：**

```
sranges(
  sid        int4      NOT NULL,
  tbl        text      NOT NULL,
  timeframe  text      NOT NULL,
  start_ms   int8      NOT NULL,
  stop_ms    int8      NOT NULL,
  has_data   bool      NOT NULL DEFAULT true,
  UNIQUE (sid, tbl, timeframe, start_ms)
)
```

与 QuestDB 的 `sranges_q` 相比：去掉 `ts`、`is_deleted`、`deleted_at` 列；加入 UNIQUE 约束支持 upsert；依赖事务保证原子性，无需 `srangesCache`。

**需要新增 PG 原生实现的函数（在 `orm/pg_srange.go` 中实现）：**

- `ListSRangesPg(ctx, sid, tbl, tf, startMs, stopMs)`：  
  标准 SQL `SELECT … FROM sranges WHERE … ORDER BY start_ms`，无 LATEST BY

- `getCoveredRangesPg(ctx, sid, tbl, tf, startMs, stopMs)`：  
  调用 `ListSRangesPg`，过滤 `has_data=true` 后调用 `mergeMSRanges`（已有函数，无需修改）

- `UpdateSRangesPg(ctx, sid, tbl, tf, startMs, stopMs, hasData)`：  
  在 PG 事务中执行：先 SELECT FOR UPDATE 读取窗口内现有段，计算合并后的新段列表，然后 DELETE 旧段、INSERT 新段；逻辑与 `UpdateSRanges` 完全等价，但用事务替代软删除

- `UpdateSRangesWithHolesPg(ctx, sid, tbl, tf, startMs, stopMs, holes)`：  
  同上，将有数据段与 holes（no_data 段）一并写入，与 `UpdateSRangesWithHoles` 逻辑等价

- `DelKInfoPg(sid, tf)`：  
  `DELETE FROM sranges WHERE sid=$1 AND tbl=$2 AND timeframe=$3`；再清 srangesCache（对 PG 可省略）

- `GetKlineRangePg(sid, tf)`：  
  `SELECT min(start_ms), max(stop_ms) FROM sranges WHERE sid=? AND tbl=? AND has_data=true`

- `GetKlineRangesPg(sidList, tf)`：  
  `SELECT sid, min(start_ms), max(stop_ms) FROM sranges WHERE tbl=? AND has_data=true AND sid IN (...) GROUP BY sid`

**srangesCache 在 TimescaleDB 模式下的处理：**

`srangesCache` 是 QuestDB 专有的 WAL 延迟补偿机制（新写入数据在 WAL 提交前查询不到）。TimescaleDB 依赖事务、写入即可见，不需要此缓存。在 TimescaleDB 模式下，`srangesCacheGet/Update/Del` 的调用通过 `if IsQuestDB` 守卫跳过，无副作用。

**业务代码分支点（最小化）：**

```
伪代码：
func getCoveredRanges(ctx, sid, tbl, tf, start, stop):
    if IsQuestDB:
        return getCoveredRangesQdb(ctx, ...)  // 现有实现
    else:
        return getCoveredRangesPg(ctx, ...)   // 新增 PG 实现

func rewriteHoleRangesInWindow(ctx, sid, tf, start, stop, holes):
    if IsQuestDB:
        return sess.UpdateSRangesWithHoles(ctx, ...)
    else:
        return sess.UpdateSRangesWithHolesPg(ctx, ...)

func updateKLineRange(sid, tf, start, end):
    if IsQuestDB:
        return sess.UpdateSRanges(ctx, ...)
    else:
        return sess.UpdateSRangesPg(ctx, ...)
```

`updateKHoles`（计算空洞列表的核心逻辑）完全不需要修改——它只是从 K 线时间戳中找出 MSRange 缺口，与数据库类型无关。

**GetKlineRange / GetKlineRanges / DelKInfo 分支：**

在 `pub_kline_queries.go` 中，这三个函数均已实现 QuestDB 路径，新增 `else` 分支调用对应的 PG 实现即可：

```
伪代码：
func (q *Queries) GetKlineRange(sid, tf):
    if IsQuestDB:
        // 现有 sranges_q LATEST BY 查询
    else:
        return GetKlineRangePg(sid, tf)

func (q *Queries) DelKInfo(sid, tf):
    if IsQuestDB:
        // 现有软删除逻辑
    else:
        return DelKInfoPg(sid, tf)
```

**BulkDownOHLCV 的范围预取（可选优化）：**

v0.2.x 中 `BulkDownOHLCV` 先批量调用 `GetKlineRanges` 预取所有 sid 的总范围，再逐个触发下载，避免每个 sid 单独查询 DB。v0.3.7 去掉了这步预取，每个 sid 在 `downOHLCV2DBRange` 内部调用 `getCoveredRanges`。

恢复 TimescaleDB 时，两种方式均可行：
- 简单路径：直接沿用 v0.3.7 策略，每 sid 独立调用 `getCoveredRangesPg`（无需修改 BulkDownOHLCV）
- 优化路径：TimescaleDB 模式下恢复 `GetKlineRangesPg` 批量预取，用单次 GROUP BY 查询替换 N 次单行查询（可在初始版本后优化）

### 2.7 K 线插入方式适配

两套后端的列集合完全一致（`sid, time/ts, open, high, low, close, volume, quote, buy_volume, trade_num`），差异仅在插入机制和时间列类型：

- **TimescaleDB**：使用 `pgx.CopyFrom`（高吞吐批量写入），时间列为 `time int8`（毫秒），冲突处理用 `ON CONFLICT (sid, time) DO UPDATE SET …`；列集合为 `(sid, time, open, high, low, close, volume, quote, buy_volume, trade_num)`，**无 `info` 字段**
- **QuestDB**：使用批量 `INSERT INTO … VALUES`，时间列为 `ts timestamp`（微秒），内置 DEDUP 幂等，无需冲突处理；列集合为 `(sid, ts, open, high, low, close, volume, quote, buy_volume, trade_num)`

`InsertKLines` 函数中通过 `IsQuestDB` 分支执行不同路径：

```
伪代码：
func (q *Queries) InsertKLines(tf string, sid int32, arr []*banexg.Kline) (int64, *errs.Error):
    if IsQuestDB:
        return insertKLinesQdb(q, tf, sid, arr)   // 批量 INSERT VALUES，ts = time.UnixMilli(ms).UTC()
    else:
        return insertKLinesPg(q, tf, sid, arr)    // CopyFrom，time = ms int8

// iterForAddKLines.Values() 在两套路径中输出的字段顺序相同：
// [sid, time/ts, open, high, low, close, volume, quote, buy_volume, trade_num]
// 区别仅在时间值类型：PG 路径传 int64(ms)，QDB 路径传 time.Time(UTC)
```

**`DelKLines` 差异：** banbotraw 中 `InsertKLines` 在遇到唯一键冲突时会先 `DelKLines`（`DELETE FROM kline_X WHERE sid=? AND time>=? AND time<?`）再重试。QuestDB 因内置 DEDUP 无需此逻辑。TimescaleDB 路径保留此冲突重试机制，但 `DelKLines` 的 SQL 需适配新时间列名 `time`（int8）。

### 2.8 未完成 K 线（kline_un）适配

**新方案字段统一：** TimescaleDB 版的 `kline_un` 表也采用拆分字段，与 QuestDB 的 `kline_un_q` 保持一致的列集合（去除 `info`，增加 `quote, buy_volume, trade_num, expire_ms`）。

**PostgreSQL `kline_un` 表结构（新）：**

```
kline_un(
  sid        int4      NOT NULL,
  timeframe  varchar(5) NOT NULL,
  start_ms   int8      NOT NULL,
  stop_ms    int8      NOT NULL,
  expire_ms  int8      NOT NULL DEFAULT 0,
  open       float8    NOT NULL,
  high       float8    NOT NULL,
  low        float8    NOT NULL,
  close      float8    NOT NULL,
  volume     float8    NOT NULL,
  quote      float8    NOT NULL DEFAULT 0,
  buy_volume float8    NOT NULL DEFAULT 0,
  trade_num  int8      NOT NULL DEFAULT 0,
  UNIQUE (sid, timeframe)
)
```

**操作差异：**

| 操作 | TimescaleDB（新方案） | QuestDB |
|------|---------------------|---------|
| 查询 | `SELECT … FROM kline_un WHERE sid=? AND timeframe=? AND start_ms>=?` | `LATEST BY sid, timeframe WHERE … AND is_deleted=false AND cast(ts as long)/1000 >= ?` |
| 写入 | `UPDATE … WHERE sid=? AND timeframe=?`，无行则 `INSERT`（upsert） | `INSERT INTO kline_un_q … VALUES (…, false)`（追加） |
| 删除 | `DELETE FROM kline_un WHERE sid=? AND timeframe=?` | `INSERT INTO kline_un_q (…, is_deleted) VALUES (…, true)` |
| 过期判断 | 读取 `expire_ms` 字段，与 `btime.UTCStamp()` 比较（与 QuestDB 路径逻辑相同） | 同左 |

`queryUnfinish`、`SetUnfinish`、`DelKLineUn` 等函数通过 `IsQuestDB` 分支路由到对应实现。`getUnFinish` 的上层逻辑（`calcUnfinishFromSubs`、过期判断、`BuyVolume` 的 `infoBy` 聚合）完全不需要修改，因为两套路径返回的 `*banexg.Kline` 结构体字段完全一致（`Quote`、`BuyVolume`、`TradeNum` 均已填充）。

**`calcUnfinishFromSubs` 的 BuyVolume 聚合：** v0.3.7 中此函数已正确处理 `infoBy`（`"sum"` 时累加 `BuyVolume`，否则取最后一个），TimescaleDB 路径无需任何修改。

### 2.9 exsymbol 表适配

- TimescaleDB（banbotraw）：`exsymbol` 表有自增 `id`（`SERIAL`），INSERT 后通过 `RETURNING id` 获取
- QuestDB：`exsymbol_q` 无自增，使用应用层在内存中维护 `maxSid` 计数器

v0.3.7 中已引入内存 `maxSid` 计数器（`exsymbol.go`），该机制对 TimescaleDB 也适用，可统一保留。但 TimescaleDB 需要在首次 `LoadAllExSymbols` 时从 `SELECT max(id) FROM exsymbol` 初始化 `maxSid`，而非依赖 `SERIAL` 自增（因为应用层已接管 sid 分配）。

### 2.10 calendars / adj_factors 适配

**字段对齐：** 新方案中 TimescaleDB 的 `calendars` 表将 banbotraw 的 `name` 字段改为 `market`，与 `calendars_q` 保持一致。`adj_factors` 表去除自增 `id` 字段。

- TimescaleDB：支持物理 DELETE + INSERT，存储结构简洁
- QuestDB：append-only 软删除（插入 `is_deleted=true` 记录）

`GetCalendars`、`SetCalendars`、`GetAdjFactors`、`DelFactors` 等函数各自维护两条路径：

```
伪代码：
func (q *Queries) SetCalendars(market string, items [][2]int64) *errs.Error:
    if IsQuestDB:
        return q.setCalendarsQdb(market, items)
    else:
        return q.setCalendarsPg(market, items)  // DELETE + INSERT，按 market 字段
```

### 2.11 SQLite 元数据（banpub.db）的角色变化

v0.3.7 将原本存于 PostgreSQL 的元数据（sranges、ins_kline、kline_un、calendars、adj_factors）迁移到QuestDB 中（`_q` 后缀表）

恢复 TimescaleDB 支持后，**SQLite `banpub.db` 仅继续承担 UI task 等轻量关系数据**；K 线元数据回归 PostgreSQL/TimescaleDB。因此：

- TimescaleDB 模式 和 QuestDB 模式：`banpub.db` 仅存 UI task 相关（沿用 `orm/sql/ui_schema.sql`）

---

## 三、文件变更范围

### 3.1 新增文件

| 文件 | 说明 |
|------|------|
| `orm/sql/pg_schema.sql` | TimescaleDB 基础表结构（含 **sranges**、exsymbol、calendars、adj_factors、ins_kline、**kline_un**（新字段）；**不含** kinfo/khole） |
| `orm/sql/pg_schema2.sql` | kline_1m~1d 超表定义（含压缩策略）；列集合为 `(sid, time int8, open, high, low, close, volume, quote, buy_volume, trade_num)`，**无 `info` 字段** |
| `orm/sql/pg_migrations.sql` | TimescaleDB 版本迁移脚本（`schema_migrations` 表 + 幂等 DDL） |
| `orm/pg_srange.go` | PostgreSQL 原生 sranges 操作实现：`ListSRangesPg`、`getCoveredRangesPg`、`UpdateSRangesPg`、`UpdateSRangesWithHolesPg`、`DelKInfoPg`、`GetKlineRangePg`、`GetKlineRangesPg` |
| `orm/pg_queries.go` | 其余 TimescaleDB 专用查询：exsymbol、calendars、adj_factors、ins_kline、kline_un（字段已统一，无 `info`） |

### 3.2 修改文件

| 文件 | 改动要点 |
|------|---------|
| `config/types.go` | `DatabaseConfig` 新增 `DbType string` 字段 |
| `orm/base.go` | 新增 `IsQuestDB bool` 全局；`pgConnPool` 恢复 5432 端口支持；`Setup` 中加入类型检测与 pg 迁移路径 |
| `orm/srange.go` | `getCoveredRanges`、`rewriteHoleRangesInWindow`（调用入口）加入 `IsQuestDB` 分支，分别路由到 QuestDB 或 PG 实现；`srangesCache` 相关调用在 PG 模式下以 `if IsQuestDB` 守卫跳过 |
| `orm/kline.go` | `InsertKLines`、`QueryOHLCV`、`QueryOHLCVBatch`、`getKLineTimes`、`getKLineTimeRange`、`UpdateKRange`（sranges 写入分支）、`updateKLineRange`（调用 UpdateSRanges 或 UpdateSRangesPg）、`getUnFinish`、`SetUnfinish` 等函数加入 `IsQuestDB` 分支 |
| `orm/kdata.go` | `downOHLCV2DBRange` 的范围判断统一使用 `getCoveredRanges`（内部已分支），无需修改下载主流程；`GetExSHoles` 的 `GetCalendars` 调用适配 |
| `orm/exsymbol.go` | `LoadAllExSymbols` 中初始化 `maxSid` 时适配两种数据库；写入分支 |
| `orm/pub_kline_queries.go` | `DelKInfo`、`GetKlineRange`、`GetKlineRanges`（加入 PG 分支）、`DelFactors`、`DelKLineUn`、`AddInsJob`、`DelInsKline` 加入 pg 路径 |
| `orm/pub_meta_queries.go` | `SetCalendars`、`GetCalendars`、`SetAdjFactors`、`GetAdjFactors` 加入 pg 路径 |
| `orm/banpub.go` | TimescaleDB 模式下 `banpub.db` 不再存储 K 线元数据，仅存 UI task |

### 3.3 不需要修改的文件

以下文件无数据库类型差异，无需修改：
- `orm/ormo/`（订单数据库，使用 SQLite，与时序数据库无关）
- `orm/ormu/`（UI 数据库，同上）
- `orm/srange_query.go`（`FindSRanges`、`ListSRangesBySid` 的 QuestDB 版本——UI 展示用；若 PG 模式下也需展示，在 `pg_srange.go` 中补对应 PG 实现）
- `updateKHoles`（核心空洞计算逻辑，完全无 DB 类型依赖，只计算 MSRange 列表）
- `mergeMSRanges`、`subtractMSRanges`（纯算法，无 DB 依赖）
- 所有 `biz/`、`strat/`、`data/`、`live/` 等上层模块（通过 `orm` 包接口调用，不感知 DB 类型）

---

## 四、实施顺序

1. **配置层**：`DatabaseConfig` 新增 `DbType`；`pgConnPool` 恢复 5432 支持；`Setup` 加入类型检测与 pg 迁移路径

2. **Schema 层**：编写三个 SQL 文件（pg_schema 含 sranges/exsymbol/calendars/adj_factors/ins_kline/kline_un，pg_schema2 含超表，pg_migrations 含版本控制）

3. **PG sranges 层**（最核心）：实现 `pg_srange.go` 中的全部函数：
   - `ListSRangesPg`、`getCoveredRangesPg`
   - `UpdateSRangesPg`、`UpdateSRangesWithHolesPg`
   - `DelKInfoPg`、`GetKlineRangePg`、`GetKlineRangesPg`

4. **srange.go / kline.go 分支**：
   - `getCoveredRanges` 加入 PG 路径
   - `rewriteHoleRangesInWindow` 加入 PG 路径
   - `updateKLineRange` 加入 PG 路径
   - `pub_kline_queries.go` 中 `DelKInfo`、`GetKlineRange`、`GetKlineRanges` 加入 PG 分支

5. **插入层**：`InsertKLines` 加 pg 路径（CopyFrom 替换批量 INSERT；列集合统一为 `sid,time,open,high,low,close,volume,quote,buy_volume,trade_num`，无 `info`）

6. **查询层**：`QueryOHLCV`、`QueryOHLCVBatch`、`getKLineTimes` 等函数加 pg 路径（时间列 `time int8` 适配；`mapToKlines` 字段映射两套后端相同，无需修改）

7. **聚合层**：`refreshAgg` 加 pg 路径（TimescaleDB 使用内联 `INSERT … SELECT … GROUP BY` 聚合，`buy_volume` 按 `infoBy` 选择 `SUM` 或 `LAST` 聚合函数）

8. **元数据层**：calendars（`market` 字段）、adj_factors（去 `id`）、exsymbol 的 pg 读写路径；`DelFactors`、`DelKLineUn`、`AddInsJob`、`DelInsKline` 加 pg 路径

9. **未完成 K 线层**：`queryUnfinish`、`SetUnfinish`、`DelKLineUn` pg 路径（新字段 `quote,buy_volume,trade_num,expire_ms`）

10. **测试验证**：TimescaleDB 模式下跑现有测试套件

---

## 五、数据迁移策略

**从 QuestDB 迁移到 TimescaleDB：**
- 利用现有 `orm/tools.go` 中的 `ExportKlineCSV` / `ImportKlineCSV` 或 protobuf 导出功能
- 先从 QuestDB 导出，再切换数据库配置，然后导入 TimescaleDB
- 该工具已存在于两个版本的 `entry/` 命令中，无需新增

**从 banbotraw（v0.2.x TimescaleDB）迁移到新方案 TimescaleDB：**
- banbotraw 的 kline 表有 `info` 字段，新方案改为 `quote + buy_volume + trade_num`
- 迁移时将 `info` 值写入 `buy_volume`，`quote` 和 `trade_num` 填零
- 可通过 `ALTER TABLE kline_Xm ADD COLUMN quote float8 DEFAULT 0, ADD COLUMN buy_volume float8, ADD COLUMN trade_num int8 DEFAULT 0; UPDATE kline_Xm SET buy_volume = info; ALTER TABLE kline_Xm DROP COLUMN info;` 原地迁移
- 或通过导出/导入工具在应用层完成字段映射

---

## 六、注意事项

1. **字段统一（核心决策）**：新方案中 TimescaleDB 的 kline 表和 kline_un 表**均采用与 QuestDB 相同的拆分字段**（`quote, buy_volume, trade_num`），彻底去除 `info` 字段。这意味着：
   - `mapToKlines`、`InsertKLines`、`calcUnfinishFromSubs` 等上层函数的字段映射代码无需为 TimescaleDB 单独适配
   - `iterForAddKLines.Values()` 的字段顺序在两套后端完全一致
   - `infoBy`（`"sum"` 或 `"last"`）的聚合逻辑在两套后端共用，无需分支
   - 历史数据迁移：从 banbotraw 迁移数据时，需将 `info` 字段值写入 `buy_volume`，`quote` 和 `trade_num` 填零；可在导入工具中处理，不影响运行时代码

2. **`refreshAgg` 的 TimescaleDB 优化路径**：banbotraw 中 `refreshAgg` 使用 PostgreSQL 内联 `INSERT INTO … SELECT … GROUP BY` 聚合，效率高于 QuestDB 的应用层聚合。新方案中 TimescaleDB 路径可恢复此优化，聚合表达式需将 `info` 替换为三列独立聚合：`SUM(quote) AS quote`、`SUM(buy_volume) AS buy_volume`（infoBy=sum）或 `LAST(buy_volume ORDER BY time) AS buy_volume`（infoBy=last）、`SUM(trade_num) AS trade_num`。

3. **并发安全与缓存**：`srangesCache` 是 QuestDB 专用的 WAL 延迟补偿缓存。TimescaleDB 使用事务保证写入即可见，不存在 WAL 延迟，因此 `IsQuestDB = false` 时 `srangesCacheGet/Update/Del` 的调用均应以 `if IsQuestDB` 守卫跳过，避免在 PG 模式下引入不必要的内存状态。PG 模式下的 `UpdateSRangesPg/UpdateSRangesWithHolesPg` 本身在事务中完成，后续查询直接可见，无需任何缓存层。

4. **端口 5432 检测**：v0.3.7 中 `pgConnPool` 有硬编码拒绝 5432 端口的逻辑，恢复 TimescaleDB 支持时必须移除该校验，改为依赖 `IsQuestDB` 标志控制行为。

5. **QuestDB 自动安装**：`questdb.go` 中的自动安装逻辑仅在 `IsQuestDB = true` 时触发，TimescaleDB 模式不受影响。

6. **banpub.db 中的 sranges/ins_kline**：v0.3.7 的 `banpub.db` SQLite 中存有 `sranges`、`ins_kline` 等表（参见 `ui_schema.sql`）用于 UI 展示。TimescaleDB 模式下，K 线范围数据存于 PostgreSQL `sranges` 表；SQLite 中的 sranges 镜像表可保留（UI 查询路径不变）或改为查询 PostgreSQL，建议初期保留 SQLite 路径以减少改动范围。注意：`sranges` 与 `sranges_q` 的字段存在差异（无 `ts`/`is_deleted`），如需 UI 统一展示，需在 banpub.go 中的 sranges 同步逻辑中做字段适配。

7. **`calendars` 表的字段差异**：banbotraw 中 `calendars` 表用 `name varchar(50)` 标识日历名称（如 `"SSE"`），而 v0.3.7 的 `calendars_q` 改用 `market SYMBOL`。新方案 TimescaleDB 版的 `calendars` 表应统一使用 `market` 字段名，与 QuestDB 版保持一致，避免上层 `GetCalendars`/`SetCalendars` 函数需要额外字段名分支。

8. **`adj_factors` 表的 `id` 字段**：banbotraw 中 `adj_factors` 有自增 `id`，v0.3.7 的 `adj_factors_q` 无 `id`。新方案 TimescaleDB 版的 `adj_factors` 表去除 `id`，与 QuestDB 版对齐，`DelAdjFactors` 等操作改为按 `(sid)` 或 `(sid, sub_id, start_ms)` 定位。

9. **`DelKLines`（表重写）的 TimescaleDB 适配**：v0.3.7 的 `DelKLines` 通过"创建新表 → 复制 → 删除旧表 → 重命名"实现大规模删除（QuestDB 不支持 `DELETE`）。TimescaleDB 支持标准 `DELETE FROM kline_X WHERE sid NOT IN (…)`，性能更优，TimescaleDB 路径应直接使用 `DELETE` 而非表重写。
