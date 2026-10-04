# 时序数据：定义一次，存取和订阅复用

所有运行时数据都使用 `orm.DataSeries.Values map[string]any`。K 线的 `close`、资金费率的 `rate`、宏观数据的 `revision` 都是字段；数值历史是这些字段的派生视图。

## 1. 定义数据

```go
var funding = orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{
    {Name: "rate", Type: "float"},
    {Name: "revision", Type: "int"},
    {Name: "announced", Type: "bool"},
    {Name: "note", Type: "string"},
})
```

定义描述源名、基础周期和字段类型，默认对应 `funding_rate_8h` 表。标的独立于数据定义：一个定义可以服务很多交易对，也可以服务自定义宏观标的。类型可选 `float/int/string/bool/json`，字段值可为 `nil`。

## 2. 绑定标的后读写

下面代码假定已有数据库会话 `ctx`、正确初始化的 `SeriesStore` 和已注册标的 `target`。

```go
rates := store.Bind(funding, target)
if err := rates.Ensure(ctx); err != nil {
    return err
}
if err := rates.WriteBatch(ctx, []*orm.DataRecord{
    {
        TimeMS: ts, EndMS: ts + 8*3600_000, Closed: true,
        Values: map[string]any{
            "rate": 0.0001, "revision": int64(9007199254740993),
            "announced": true, "note": nil,
        },
    },
}); err != nil {
    return err
}
rows, err := rates.Read(ctx, startMS, endMS, 500)
if err != nil {
    return err
}
for _, row := range rows {
    rate, err := row.FloatValue("rate")
    if err != nil {
        return err
    }
    note, present := row.Values["note"]
    _, _, _ = rate, note, present
}
```

`Bind` 只保留定义和标的引用，没有数据库操作，也不复制每行的 Values。`Write/WriteBatch` 接受持久行；已有运行时事件可直接使用 `WriteSeries/WriteSeriesBatch`。`Missing/FillMissing/Coverage/UpdateCoverage/Delete` 沿用原 SeriesStore 语义。读写时的参数校验、覆盖范围处理和 QuestDB 可见性机制均委托原实现。

高吞吐导入优先批量写入。`TimeMS/EndMS` 单位为毫秒，表示 `[TimeMS, EndMS)`，必须满足 `EndMS > TimeMS`；不要用当前时间替代原始数据时间。绑定后不要并发修改 schema 或标的对象，Values 的所有权规则也与原接口相同。

数据库按 schema 存储字段：没有声明的字段不会因出现在 map 中而自动新增数据库列；未提供的 schema 字段可能存为 NULL。运行时 map 的“缺失”与“显式 nil”可区分，但固定列数据库往返不能恢复写入前的缺失状态。

## 3. 同一订阅用于声明和读取

```go
// 构建策略时创建，之后作为只读模板供各 job 使用。
ratesSub := strat.NewDataSub(funding)
ratesSub.WarmupNum = 30
ratesSub.Fields = []string{"rate", "revision", "note"}
ratesSub.SeriesFields = []string{"rate"}

strategy := &strat.TradeStrat{
    OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
        return []*strat.DataSub{ratesSub}
    },
    OnData: func(job *strat.StratJob, event strat.DataEvent) {
        if !event.IsMain() {
            return
        }
        latest := job.Data(ratesSub)
        if latest == nil || latest.DoneMS == 0 {
            return // 订阅可能已配置，但第一条记录尚未到达。
        }
        rateHistory := latest.Series("rate")
        revision, present := latest.RawValue("revision")
        _, _, _ = rateHistory, revision, present
    },
}
_ = strategy
```

省略 `ExSymbol` 表示当前 job 的标的；跨标的时在订阅上显式设置 `ExSymbol`。同一个订阅用于 `OnDataSubs` 和 `job.Data(sub)`，无需再次填写周期、source 和 sid。读取的是当前 job 的 DataHub，不会重新查询数据库。这个方法不启动订阅，也不会等待数据；在配置之后收到事件时视图才会更新。

需要每次数据更新立即计算时，直接在 `OnData` 中处理对应 source 的事件，或使用已有 `RouteData`。上面的例子选择在主 K 线回调中消费最近已到达的资金费率。业务若要求同一时刻所有闭合周期到齐，可以检查 `job.DataHub.AllReady()`；稀疏发布的数据应按自己的新鲜度规则判断，不能把 `AllReady` 当作任意数据的通用等待器。

| 入口 | 含义 |
|---|---|
| `Fields` | 从源中读取的字段投影；空值采用源默认字段 |
| `SeriesFields` | 维护数值历史、供 banta 指标使用的字段；会自动并入读取投影 |
| `Series("rate")` | 派生的数值历史，未建立时返回 nil；NULL/缺失历史点用 NaN 表示 |
| `RawValue("revision")` | 最新原始值和存在标记；保留 int64 精度、bool、string、JSON 和显式 nil |
| `Float64/Int64/String` | 便捷转换；需要严格校验或区分 NULL 时使用 RawValue |

未指定 `SeriesFields` 时默认给浮点字段维护数值历史。大整数计数、修订号等保留在原始值中；显式将其选为数值历史会转成 float64，可能失去精度。不要仅凭 `Float64(...) == 0` 判断数据已存在。

## 4. 自动补齐和实盘推送

已有 `data.RegisterFuncDataSource(funding, fetchHistory, subscribeLive)` 适合简单源；复杂分页、限流和订阅生命周期可实现 `data.DataSource`。注册时需要历史抓取函数；实时订阅函数可省略，但省略并不意味着框架会自动轮询该源。

`fetchHistory` 接收订阅和 `[startMS,endMS)`，返回 `[]*orm.DataRecord`。框架负责查询缺口、写入、覆盖范围更新及回测回放。实时函数通过 `sink.Emit(sub, rows)` 发送数据；可保留批量形式，避免每条消息重复建立上下文。

手动存取不要求注册数据源；接入策略的自动补齐和回放需要注册。显式运行时使用其 `DataSourceCatalog` 和 `orm.NewSeriesStore(orm.NewSeriesRepo(storage))`；传统单运行时入口可使用 `RegisterFuncDataSource` 和 `DefaultSeriesStore()`。不要在运行中的多个会话之间共享可变源实例。

## 5. K 线扩展字段

数据与已有 K 线逐行对齐时，沿用 `NewKLineSeriesInfo` 和 `KLineSeriesStore`。这类写入更新已有 K 线行，独立 `SeriesStore` 则写入完整独立记录，两者的写入语义不同。

```go
oi := &strat.DataSub{
    Source: "kline", TimeFrame: "1h",
    SeriesFields: []string{"open_interest"},
}
// 在 OnDataSubs 返回 oi；在回调中使用 job.Data(oi)。
```

K 线扩展定义的 Name 可以是业务标签，不是独立 source 名；订阅必须使用 `Source: "kline"`。不要直接把带其他 Name 的 `NewKLineSeriesInfo` 传给 `NewDataSub`。主 K 线默认字段会与扩展字段合并，原始扩展值继续通过同一个 Values/DataHub 传递。

## 6. 灵活性的实际边界

“任意时序”同时保留任意已声明字段和引擎各自的时间合同。Subscription 支持 FrequencyBar 与 FrequencyEvent；event 使用 TimeFrame="event"，计数预热需要 ObservationWarmupSource 按真实观测查找起点，合法声明不代表所有历史 reader 都支持它。SeriesInfo 与订阅周期仍需一致，主交易角色仍以 kline source 为准。

因子 VersionRecord/VersionStore/Snapshot 提供事件、可见、接收时间和修订版本的 PIT 流程；普通最新值 SeriesStore 不能据 K 线时间推导这些证据。使用数据库最新值进行因子回测必须明确 static-approximation；需要严格 PIT 时提供有版本和可见性证据的历史输入。TS DataHub 是事件驱动的最近值/数值历史视图，不是任意源的 as-of join。

实时逐笔成交和盘口仍提供 `OnWsTrades` / `OnWsDepth` 专门回调；`OnWsData` 当前主要用于 WebSocket K 线事件，不能把它当成自动接管所有实时数据的入口。

需要这些能力时，应先明确事件时间、可用时间、修订版本和聚合语义，再扩展现有链路。自定义聚合使用 `orm.RegisterAggRule` 与 ExSymbol.AggRules，未配置扩展列默认 last；first/last 保留所选原始值，数值规则有转换/NULL 校验。feeder 复权调整 open/high/low/close/volume/buy_volume，其他扩展列保持原值。不要丢弃非 OHLCV 字段或把 map 收窄为固定 K 线模型。

## 7. 序列化和写后读

Spider 的 NotifySeries/SeriesMsg 已直接携带 DataSeries.Rows，不再传 Arr []*banexg.Kline。BanIO 内部消息外壳使用 gob，payload 经 JSON、压缩及可选加密；DataSeries.Values 的 JSON map 不会自动保留任意整数宽度或自定义 Go 类型，类型严格的源需 schema/解码校验，因子 JSON archive 导入使用 schema。进程内 startup 缓冲采用递归复制，不经过 JSON，可保留具体类型、typed nil、NULL 与缺键。

QuestDB INSERT/CTAS 成功后不保证立即可读。依赖后续读取时等待目标行、时间戳、范围或记录数可见，替换前验证新表预期快照；超时保留恢复标记。不要依据一次空读清理恢复状态或删除旧表。同进程元数据优先用定向可见性等待或所属缓存/锁。

## 2026-10-04 双引擎使用入口

run_policy.engine 接受 time_series/factor，省略时为时序。原生多因子图、表达式、PIT、成熟标签、weights/events、混合账户和实时生命周期见[多因子与截面指南](../bandoc/zh-CN/guide/factor.md)及[API](../bandoc/zh-CN/api/factor.md)。逐包结论和本次验证见[重构记录](strategy_engine_refactor.md)。

execution.live_provider: verified-session 只是用户工厂示例名，必须先注册 entry.RegisterFactorLiveBinding("verified-session", factory) 并提供真实证据。内置 empty/banexg 或未注册工厂缺能力时明确失败，不自动降级 paper；factor trade --dry-run 是历史模拟。最新值数据库必须显式 static-approximation；任意字段/NULL 继续通过 DataSeries.Values。
