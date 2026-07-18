# BanBot 关系型元数据迁移 QuestDB 方案

## 1. 背景与目标

**根本问题：** `banpub.db`（SQLite）在多进程并发写入时频繁触发 `database locked` 错误。SQLite 的文件级写锁机制决定了它不适合多进程高并发场景，尤其是以下三张表：
- `sranges`：高频"删旧+插新"事务，核心锁竞争源
- `kline_un`：实盘每分钟多品种并发 UPSERT
- `ins_kline`：并发下载抢锁、释放锁，第二锁竞争源

**目标：** 将 `banpub_schema.sql` 中除 `task` 外的所有表迁移至 QuestDB，彻底消除并发写锁冲突。QuestDB 可被外部机器直接通过 PGWire 访问，无需在外部安装 SQLite。

---

## 2. 各表业务操作频率分析

| 表名 | 读 | 写 | 并发风险 |
|------|----|----|---------|
| `exsymbol` | **极高**（每次 K 线操作都查） | 低（新品种上市） | 低 |
| `calendars` | 中 | 低（批量初始化） | 低 |
| `adj_factors` | 低（仅 A 股期货） | 低（换月时批量） | 极低 |
| `sranges` | **高** | **高**（每次 K 线下载后必调） | **核心锁竞争源** |
| `ins_kline` | 中 | 中高（并发下载） | **第二锁竞争源** |
| `kline_un` | 高 | **高**（实盘每分钟多品种并发） | **第三锁竞争源** |

---

## 3. QuestDB 表设计原则

- **不使用物理 DELETE**：所有删除通过追加 `is_deleted=true` 行实现逻辑删除，QuestDB 不支持高效的单行删除
- **WAL + DEDUP UPSERT**：利用 WAL 表幂等写入能力实现 UPSERT 语义，彻底消除写锁
- **DEDUP 键必须包含 designated timestamp**（QuestDB 强制要求），且必须能唯一标识一个逻辑实体的一个版本
- **LATEST BY 语义**：每个逻辑实体取最新版本，过滤 `is_deleted=false`
- **ts 统一使用 UTC 微秒时间戳**（QuestDB TIMESTAMP 类型）

---

## 4. 各表关键设计决策

### `exsymbol_q`
DEDUP 键为 `(sid, ts)`。写极低频，读极高频，追加 + `LATEST BY sid` 即可。

### `calendars_q`
DEDUP 键必须包含 `start_ms`。原因：同一市场批量写入多条日历时，各条 ts 相同但 start_ms 不同；若 DEDUP 键仅含 `(market, ts)` 会将同批次多条误合并为一条。

### `adj_factors_q`
DEDUP 键必须包含 `start_ms`，且**不能用 `LATEST BY sid`**（只取一条），必须用 `LATEST BY sid, sub_id, start_ms`。原因：同一 sid 有多个不同 `start_ms` 的因子节点，查询时需全量获取所有节点。

### `sranges_q`
原 SQLite 方案在事务内"SELECT → 计算合并 → DELETE 旧行 → INSERT 新行"，写锁竞争严重。新方案改为纯追加无锁：
1. 读取有效段（`LATEST BY ... WHERE is_deleted=false`），在 Go 内存中执行合并计算（`mergeMSRanges` 逻辑不变）
2. 对需要被合并/覆盖的旧段追加逻辑删除行
3. INSERT 合并后的新段

DEDUP 键为 `(sid, tbl, timeframe, start_ms, ts)`，用 `start_ms` 标识段的身份，`ts` 记录版本。

**WAL 延迟问题**：写入后立即读存在 < 100ms 不可见窗口。在进程内维护 `srangesCache`，`UpdateSRanges` 写入后同步更新缓存，`ListSRanges` 优先读缓存兜底。

### `ins_kline_q`
原 `DelInsKline(id int64)` 的 `id` 是 SQLite AUTOINCREMENT，改为 QuestDB 后任务 ID 改为微秒时间戳 `ts`（即 `AddInsKline` 的创建时间）。签名变更为 `DelInsKline(sid, tf, ts)`，调用方（`kdata.go`、`kline.go` 中的 defer）需同步更新。使用 DAY 分区，旧分区可直接 `DROP PARTITION` 清理，无需额外 compact。

### `kline_un_q`
最高频 UPSERT，WAL DEDUP 天然适配。DEDUP 键 `(sid, timeframe, ts)` 中 `ts` 为 bar 开始时间，同一 `(sid, timeframe, bar_ts)` 组合的新写入自动覆盖旧值。

---

## 5. `sid` 自增 ID 方案（纯内存计数器）

QuestDB 不支持 `SEQUENCE` / `AUTOINCREMENT`，通过进程内计数器解决：

- `LoadExgSymbols` 启动时从 QuestDB 加载所有现存 sid，同步更新进程内 `maxSid`
- `AddSymbols` 在 `symbolLock` 持有期间：先查 QuestDB `MAX(sid)` 同步一次（应对极罕见多进程并发），再在内存中自增分配新 sid
- 整个"查重 → 分配 ID → 写库"流程已由 `symbolLock` 保护，进程内严格串行，无需 atomic

**多进程冲突兜底**：`LoadExgSymbols` 加载后检测同 sid 是否映射到不同 `(exchange, market, symbol)`；若发现则追加修正行（新 sid）并记录 warn 日志。实际工程中新品种上市由单一 bot 协调进程负责，此竞争不会发生。

---

## 6. 表膨胀治理（自动 Compact 机制）

### 问题
逻辑删除会持续积累无效行（`is_deleted=true` 的旧版本），会增大存储体积并拖慢 `LATEST BY` 扫描速度。

### 触发策略：后台巡检 + 分级缓存

自动 Compact 只在 `web` 和 `spider` 子命令启动。回测、优化和一次性数据工具不会启动维护 Goroutine。

每张表按自身写入特征设置快速检查周期和强制全量检查周期：`sranges_q` 为 30 分钟/2 小时，`kline_un_q` 为 1 小时/4 小时，`ins_kline_q` 为 2 小时/6 小时，其余低频表为 6-12 小时/24 小时。

快速检查查询 QuestDB 内存中的 `tables()` 指标，包括近似行数、待 apply WAL 行数和事务号。只有行数变化或进程内累计版本数超过阈值时，才执行 `count(*)` 和 `LATEST BY` 有效行统计。即使当前进程没有写入，强制全量检查周期到期后也会扫描真实表状态，因此可以发现其他进程产生的膨胀。

**触发阈值**：`valid_rows / total_rows < 0.35` 且 `total_rows >= 500`（小表 Compact 收益极低）。

### Compact 执行流程

快速检查确认可能需要 Compact → 获取跨进程独占表租约和进程内表写锁 → 等待源表 WAL 全部 apply → 锁内重新统计 → 建新表并验证有效快照 → 旧表重命名为备份 → 新表重命名为正式表 → 删除备份。

**并发安全**：所有 QuestDB 模式下的协作进程都会参与锁协议，不取决于该进程是否启动自动 Compact。普通业务读写在完整逻辑操作期间持有同机共享文件租约和进程内读锁；Compact 与破坏性维护持有同一张表的独占文件租约和进程内写锁。获得独占租约后才等待 WAL、重新统计、生成快照和换表，因此最终事务检查到 `RENAME` 之间不会再有协作进程读写。

QuestDB 启动迁移也按固定顺序获取全部受管表的共享租约后才执行 DDL，避免 `CREATE TABLE IF NOT EXISTS` 在 Compact 的两次重命名之间创建空同名表。

**失败恢复**：旧表不会被直接删除。若新表激活失败，立即将备份表恢复为原表；若恢复也失败，日志保留备份表和临时表名称用于恢复。

**与 WAL 延迟的交互**：Compact 获得独占租约后等待 `wal_txn == table_txn` 且待 apply 行数为零。生成临时表后再次比较源表 WAL 事务号；若存在未遵循租约协议的外部客户端写入并改变事务号，放弃本次临时表并稍后重试。

### 各表补充清理策略

- `ins_kline_q`：DAY 分区粒度细，旧日分区可直接 `DROP PARTITION`，作为 Compact 之外的补充手段
- `kline_un_q`：旧月分区可 `DROP PARTITION`

---

## 7. SQLite `banpub.db` 精简

迁移完成后，`banpub.db` **只保留 `task` 表**（UI 回测任务，单进程访问，结构复杂不适合 QuestDB）。SQLite 依赖仅限于 UI 回测任务管理，与业务数据读写完全解耦。

---

## 8. 外部访问架构

```
外部机器 ──PGWire:8812──→ QuestDB（所有业务数据）
Bot 本地 ──────────────→ QuestDB（同上）
Bot 本地 ──────────────→ banpub.db（仅 task，本地单进程，无并发冲突）
```

---

## 9. 风险与注意事项

1. **WAL 写入延迟**：< 100ms，`sranges_q` 写后立即读需内存缓存兜底（见第 4 节）。

2. **`sranges_q` 并发写冲突的残余风险**：两个进程同时读取有效段后各自计算合并，可能产生 ABA 问题（QuestDB 无行级锁）。缓解：将合并计算移到单一协调进程，或接受极低概率的短暂不一致（下次写入自动修正）。

3. **`ins_kline` ID 类型变化**：从 SQLite AUTOINCREMENT int64 改为微秒时间戳，所有调用 `DelInsKline(id)` 的位置需同步更新签名。

4. **`adj_factors_q` 查询方式**：必须用 `LATEST BY sid, sub_id, start_ms`，不能用 `LATEST BY sid`，否则只取最新一条因子导致数据错误。

5. **`calendars_q` 合并逻辑**：原 `SetCalendars` 的区间合并逻辑在 Go 层完成后再写 QuestDB，写入前先批量逻辑删除旧行。

6. **Compact 失败容错**：`DROP TABLE` 前必须验证新表行数 > 0，失败时记录 error 并告警，避免数据丢失。
