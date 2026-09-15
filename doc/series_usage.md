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

当前“任意时序”主要指任意已声明字段及类型，不代表所有事件时间语义都已实现。独立源订阅仍要求合法基础周期，并与源定义周期一致；主交易回调角色仍以 K 线为主。宏观修订、延迟公布和不规则事件需要由源正确编码可用时间，避免提前使用尚未发布的数据。框架目前没有通用的双时间（发生时间/可知时间）查询或 as-of join API。

实时逐笔成交和盘口仍提供 `OnWsTrades` / `OnWsDepth` 专门回调；`OnWsData` 当前主要用于 WebSocket K 线事件，不能把它当成自动接管所有实时数据的入口。

需要这些能力时，应先明确事件时间、可用时间、修订版本和聚合语义，再扩展现有链路。自定义聚合可继续使用 `orm.RegisterAggRule` 与标的聚合规则；不要通过丢弃非 OHLCV 字段来换取速度。
