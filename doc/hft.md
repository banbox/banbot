## 高频数据的存储

本页保留早期格式与数据库性能比较，数字属于当时的环境，不代表当前版本的性能保证。当前支持 PostgreSQL 和 QuestDB，任意字段读写模型为 `orm.DataSeries.Values map[string]any`；数据注册、存储与查询请使用 [时序数据说明](series_usage.md) 和 [数据库说明](db.md)。

| 格式            | 读取耗时  | 磁盘占用 |
|---------------|-------|------|
| csv.zip       | 120ms | 30M  | 
| gob([]string) | 100ms | 200M |
| gob([]Trade)  | 60ms  | 200M |
| binary        | 60ms  | 100M |

## 时序数据库
https://github.com/banbox/banbot/discussions/128

从timescaledb改为了questdb

26品种1年5m回测，273.5W根K线，timescaledb用时38s，questdb用时42s

单品种1年1m回测，52.6W根K线，timescaledb用时7.2s，questdb用时9.3s

不过单K线写入速度提升8倍。批量1000个写入提升3倍。

5线程写入速度提升类似。
