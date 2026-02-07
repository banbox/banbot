## 高频数据的存储

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

