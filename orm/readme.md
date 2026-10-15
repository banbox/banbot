# 数据库概述
数据分为任意时序数据、交易/账户状态和 UI 任务元数据。
时序存储支持 QuestDB（PGWire）与 TimescaleDB；K 线是默认 series，`DataSeries.Values map[string]any` 同时传递扩展列与自定义字段，保留具体类型、NULL 和缺失键。`sranges` 记录已下载/无数据区间，允许数据不连续。
为确保灵活性，交易数据(ormo)和UI相关数据(ormu)使用独立的sqlite文件存储。  
ormo/ormu依赖orm，不可反向依赖，避免出现依赖环

因子历史读取需要区分 latest-value 的 `static-approximation` 和严格 PIT 版本/可见性证据。`RecordToSeries` 转换借用 Values，不代替异步队列所需的独立复制。QuestDB WAL 写后读需等待目标可见；表替换必须先验证快照，超时保留恢复标记。共享执行账户的 MemoryStore/SQL ledger 与 ormo 的时序订单投影有不同职责。

见[自定义数据](../doc/custom_data.md)、[因子指南](../bandoc/zh-CN/guide/factor.md)和[重构记录](../doc/strategy_engine_refactor.md)。


## 统一读写与处理

独立序列使用 NewSeriesInfo + NewSeriesStore(NewSeriesRepo(storage))，Bind(info,target) 可复用定义与标的，再 Write/Read/Missing/FillMissing。已有K线扩展使用 KLineSeriesStore，更新对应已有行；GetSeriesFields/QuerySeriesFields 返回完整字段投影。旧 GetOHLCV 和 protobuf K线导出是固定字段兼容工具，不能代替任意列迁移。

运行时Values保具体类型；数据库schema会规范化float/int并编码JSON，缺键写固定列后可能成为NULL。聚合按 ExSymbol.AggRules/RegisterAggRule，first/last与数值规则有不同语义；feeder复权只调整支持字段，其他列不被丢弃。Spider/BanIO的JSON payload也需要schema/解码类型校验。

## 从proto生成go代码

以下仅生成默认K线protobuf协议，不代表任意DataSeries.Values序列化：
安装protoc:
```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```
生成：
```shell
protoc --go_out=. --go_opt=paths=source_relative kdata.proto
```
注意将生成后的`kdata.pb.go`中`package __`改为`package orm`
