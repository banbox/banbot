# 自定义时序数据

banbot 不只支持 K 线。资金费率、持仓量、链上指标、宏观数据或您的计算指标，都可以按时间序列存储，并在回测和实盘中由策略统一消费。

## 两种接入方式

| 场景 | 推荐方式 |
| --- | --- |
| 数据与已有 K 线一一对应，例如每根 K 线的持仓量 | 使用 `KLineSeriesStore` 为 `kline_<timeframe>` 增加扩展列 |
| 数据有独立周期或独立字段，例如每 8 小时的资金费率 | 创建独立 `SeriesInfo`，注册 `DataSource` |

两种方式都使用 `DataSeries` 在运行时传递。新策略应使用 `OnDataSubs` 声明需求，并在 `OnData` 中消费；`OnBar` 仍用于兼容传统 OHLCV K 线策略。

## 数据模型

每个独立序列由 `orm.SeriesInfo` 描述。`name + timeframe` 默认生成表名，例如 `funding_rate_8h`；每条数据都关联一个 `ExSymbol` 的 `sid`，并有开始、结束时间和字段值。

```go
info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{
    {Name: "rate", Type: "float", Role: "value"},
    {Name: "next_rate", Type: "float", Role: "value"},
})
```

字段类型支持 `float`、`int`、`string`、`bool` 和 `json`。TimescaleDB 将 `json` 保存为 `JSONB`；QuestDB 将其保存为字符串。请为同一个 source 保持固定的字段和周期，且 source 名称必须唯一。

运行时事件的核心字段为：

- `Source`：数据源名称，例如 `funding_rate`
- `Sid` / `ExSymbol`：数据绑定的标的
- `TimeMS`、`EndMS`、`TimeFrame`：数据覆盖的时间区间和基础周期
- `Values`：按字段名保存的值

## 注册数据源

最简单的方式是使用函数式数据源。历史抓取函数负责返回指定时间范围的数据；实时订阅函数可选，未提供时该数据源仍可用于回测和启动时回填。

```go
func init() {
    info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{
        {Name: "rate", Type: "float", Role: "value"},
    })
    err := data.RegisterFuncDataSource(info,
        func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
            // 从供应商拉取数据，并转换为毫秒时间戳。
            return []*orm.DataRecord{{
                TimeMS: startMS,
                EndMS:  startMS + 8*60*60*1000,
                Closed: true,
                Values: map[string]any{"rate": 0.0001},
            }}, nil
        },
        func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
            // 收到实时数据后，向对应订阅发送一批 DataRecord。
            // return sink.Emit(subs[0], rows)
            return nil
        },
    )
    if err != nil {
        panic(err)
    }
}
```

历史数据会由 banbot 自动按缺口补齐并写入数据库。实时数据源必须对收到的每条订阅调用 `sink.Emit(sub, rows)`；不要绕过该入口直接向策略发送数据。注册发生在策略包初始化期间，因此运行机器人前，包含注册代码的策略包必须已被编译进可执行文件。

若只需要手动存取，不需要自动回填或实时订阅，可直接使用 `orm.DefaultSeriesStore()` 的 `Write`、`WriteBatch`、`Read`、`Missing` 和 `Delete`。抽象指标也必须先绑定 `ExSymbol`，可通过 `orm.EnsureExSymbol(...)` 创建或复用。

## 在策略中订阅和消费

在 `OnDataSubs` 返回订阅，在 `OnData` 接收处理后的字段：

```go
strat.AddStrat(&strat.TradeStrat{
    Name: "funding_demo",
    OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
        return []*strat.DataSub{{
            Source:    "funding_rate",
            ExSymbol:  job.Symbol,
            TimeFrame: "8h",
            WarmupNum: 30,
            Fields:       []string{"rate"},
            SeriesFields: []string{"rate"},
        }}
    },
    OnData: strat.CustomData(func(job *strat.StratJob, data strat.DataEvent) {
        if data.Source != "funding_rate" {
            return
        }
        rate := data.Float64("rate")
        if !job.DataHub.AllReady() {
            return
        }
        latest := job.DataHub.Get(data.TimeFrame, data.Source, data.Sid)
        _, _ = rate, latest
    }),
})
```

`DataEvent` 嵌入了 `*DataFields`，所以可直接使用 `Series`、`Float64` 等原有字段方法。没有辅助或自定义订阅时可直接赋值 `OnData`；需要过滤全部 K 线或自定义时序时，使用 `KlineData` 或 `CustomData`；不同数据类型需要不同逻辑时，使用 `RouteData(DataHandlers{Main: ..., Info: ..., Custom: ...})`。`OnData` 已定义时，同一主事件不会再重复触发旧 `OnBar`。

`TimeFrame` 必须与数据源的 `SeriesInfo` 一致。启动时 banbot 会按 `(source, sid, timeframe)` 合并重复订阅，合并字段列表，并采用最大的 `WarmupNum`。回测会先回填再按时间顺序将数据与 K 线一起回放；实盘会先补齐历史，再激活实时订阅。未注册 source、周期不一致或声明订阅却未提供 `OnData`/兼容回调都会导致启动失败。

`Fields` 控制读取投影；`SeriesFields` 控制维护为 `banta.Series` 的字段，并会自动加入读取投影。未配置 `SeriesFields` 时默认将 `float*` 字段转换为 Series，其他字段保留为最新值。

`job.DataHub.Get(timeframe, source, sid)` 获取对应的 `DataFields`。`job.DataHub.AllReady()` 只检查当前事件时间上应当闭合的周期，可用于等待同一时刻的多个数据源全部更新后再执行策略逻辑。

## K 线扩展字段

若数据严格与 K 线时间戳对齐，可使用 `orm.NewKLineSeriesInfo(...)` 和 `orm.NewKLineSeriesStore(...)` 写入扩展列。策略仍通过相同的订阅入口读取：

```go
&strat.DataSub{
    Source: "kline", ExSymbol: job.Symbol, TimeFrame: "1h",
    SeriesFields: []string{"open_interest"},
}
```

K 线默认 OHLCV 字段会保留，扩展字段可从 `data.Series("open_interest")` 或 `data.Float64("open_interest")` 读取。扩展列不能使用 `sid`、`time`、`ts`、`open`、`high`、`low`、`close`、`volume`、`quote`、`buy_volume` 或 `trade_num` 等内置字段名。

## 查看数据

已注册的自定义数据可在 WebUI/Dashboard 的“数据”页面查看。查看器只读，支持按 source、sid、timeframe、时间范围和字段筛选；写入、补齐和删除应继续使用 `SeriesStore` 或数据源运行时。

数据库后端的部署和配置请参阅[数据库](./database.md)。
