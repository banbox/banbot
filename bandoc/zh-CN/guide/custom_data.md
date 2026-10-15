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
- `Values map[string]any`：按字段名保存的原始值；整数、字符串、布尔、JSON、显式 nil 与缺键都可进入运行时，不限于 OHLCV

原始字段用 `DataFields.RawValue(name)` 的值和存在标记读取；数值历史 `Series(name)` 是派生视图，NULL/缺失点为 NaN。显式把大整数选入 Series 会转成 float64，可能丢精度。数据库只存已声明字段，缺键写入固定列后可能成为 NULL；JSON 网络传输也不能自动保持 Go 整数宽度，不能把这些边界当成原始 map 的无损复制。

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

示例函数体只是接入骨架：实际实现必须抓取请求范围内的数据，并在实时函数中启动由 ctx 取消的生产者；不能以 `return nil` 代替实际订阅。`RegisterFuncDataSource` 已注册实例工厂。复杂源应使用 `data.RegisterDataSourceFactory(name, factory)`，每次返回独立实例；仅注册共享实例不能满足显式 Runtime 的隔离要求。

历史数据会由 banbot 自动按缺口补齐并写入数据库。实时数据源必须对收到的每条订阅调用 `sink.Emit(sub, rows)`；不要绕过该入口直接向策略发送数据。注册发生在策略包初始化期间，因此运行机器人前，包含注册代码的策略包必须已被编译进可执行文件。

定义和标的可通过 `store.Bind(info, target)` 绑定一次，再调用 `Write/Read/Missing/FillMissing/Coverage`。该方法不执行 I/O，不复制 schema、标的或 Values；初始化后不要并发修改这些定义。`strat.NewDataSub(info)` 可从独立序列创建订阅模板，再通过 `job.Data(sub)` 读取当前 job 的 DataHub；它不发起数据库查询或等待数据。省略模板的 ExSymbol 时使用 job 的标的。

若只需要手动存取，不需要自动回填或实时订阅，可直接使用 `orm.DefaultSeriesStore()` 的 `Write`、`WriteBatch`、`Read`、`Missing` 和 `Delete`。抽象指标也必须先绑定 `ExSymbol`，可通过 `orm.EnsureExSymbol(...)` 创建或复用。

## 在策略中订阅和消费

在 `OnDataSubs` 返回订阅，在 `OnData` 接收处理后的字段：

```go
func init() {
	strat.RegisterStrategy("funding_demo", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			Name: "funding_demo",
			OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{
					Source:       "funding_rate",
					ExSymbol:     job.Symbol,
					TimeFrame:    "8h",
					WarmupNum:    30,
					Fields:       []string{"rate"},
					SeriesFields: []string{"rate"},
				}}
			},
			OnData: strat.RouteData(strat.DataHandlers{
				Custom: func(job *strat.StratJob, data strat.DataEvent) {
					if data.Source != "funding_rate" {
						return
					}
					rate := data.Float64("rate")
					if !job.DataHub.AllReady() {
						return
					}
					latest := job.DataHub.Get(data.TimeFrame, data.Source, data.Sid)
					_, _ = rate, latest
				},
			}),
		}
	})
}
```

`DataEvent` 嵌入了 `*DataFields`，所以可直接使用 `Series`、`Float64` 等原有字段方法。没有辅助或自定义订阅时可直接赋值 `OnData`；需要过滤或分别处理主周期、辅助 K 线和自定义时序时，使用 `RouteData(DataHandlers{Main: ..., Info: ..., Custom: ...})`。`OnData` 不能与 `OnBar` 或 `OnInfoBar` 同时配置；策略构建会直接报错，避免主 K 线逻辑被静默跳过或重复执行。

`TimeFrame` 必须与数据源的 `SeriesInfo` 一致。规则周期使用 FrequencyBar；不规则流可声明 `TimeFrame: "event"`、`Frequency: orm.FrequencyEvent`，但仍需可用 reader。event 的 WarmupNum 表示观测条数，source 必须实现 `ObservationWarmupSource.WarmupStart` 解析真实历史起点；缺少此能力会失败，不能用虚构的事件周期补齐。启动时 banbot 会按 `(source, sid, timeframe)` 合并重复订阅，合并字段列表，并采用最大的 `WarmupNum`。回测会先回填再按时间顺序将数据与 K 线一起回放；实盘会先补齐历史，再激活实时订阅。未注册 source、周期不一致或声明订阅却未提供 `OnData`/兼容回调都会导致启动失败。

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

## 聚合、复权和生命周期

自定义列随 Values 经过 feeder 和序列化，不应先缩成 K 线再恢复。`ExSymbol.AggRules` 配置列级 first/last/min/max/sum/avg/mid，扩展列默认 last；可用 `orm.RegisterAggRule` 注册规则。first/last 保留选中的原始值，数值规则有转换与 NULL 校验，业务应按字段语义选择。复权只调整支持的 open/high/low/close/volume/buy_volume，自定义字段原值仍保留，不自动使用价格倍率。

双引擎通过 SubscriptionPlan 合并数据需求。实盘准备新代时先预热并暂存实时批次，启动缓冲深复制 Values 与元数据；预算不足或校验失败时回滚候选代。受管理源返回 Stop/Join 句柄，所属 owner 封闭输入并等待已接纳回调；不要让一个源实例的 Stop 关闭其他任务。

QuestDB WAL 写入异步可见：写后需要读取时等待目标行/范围可见，表替换先验证新表快照，超时保留恢复标记。数据库部署和配置请参阅[数据库](./database.md)。

## 多因子与截面引擎

自定义列可经 DataHub 进入时序，也可经 factor.expr bindings/field 进入因子图。源/schema/source version、事件与可见/接收时间必须一致，K线时间戳本身不能证明 PIT。跨引擎保留具体类型、NULL 和缺失键；不要把字符串/NULL 隐式当作零。

参见[多因子与截面指南](./factor.md)和[因子 API](../api/factor.md)。
