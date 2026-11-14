# btime 包

btime 包提供了时间处理相关的功能。

## 运行时钟

每个 `runtime.Runtime` 持有独立的 `btime.ClockState`：回测推进自己的模拟时间，实盘读取实时 UTC 时间。任务内的业务代码应通过 Runtime 传入的 ClockState 读取或推进时间，避免不同任务互相影响。

下方的包级时间函数保留给旧调用链；它们不能作为多个 Runtime 并行运行时的任务时钟。

## ClockState 方法与回测示例

`NewClockState(backtest bool, loc *time.Location) *ClockState` 创建时钟。`TimeMS()` 返回毫秒，`Time()` 返回浮点秒，`Now()` 返回 *time.Time。SetTimeMS 设置模拟时间，AdvanceMS 返回推进后的毫秒时间。未设置回测时间时首次读取会用真实时间初始化，因此手动回放前应先设置起点。下面为宿主/测试示例，正常策略不要自行推进框架时钟。

```go
clock := btime.NewClockState(true, time.UTC)
startMS, err := btime.ParseTimeMS("2025-01-01 00:00:00")
if err != nil { panic(err) }
clock.SetTimeMS(startMS)
nextMS := clock.AdvanceMS(60_000)
fmt.Println(nextMS - startMS) // 60000
```

## 公开方法

### TimeMS
获取兼容调用链当前时间戳（毫秒）；回测取兼容模拟时钟，实盘取真实时间。

返回：
- `int64` - 当前时间戳

### UTCTime
获取当前UTC时间的10位秒级浮点数时间戳。

返回：
- `float64` - 10位秒级浮点数时间戳

### UTCStamp
获取当前UTC时间的13位毫秒时间戳。

返回：
- `int64` - 13位毫秒时间戳

### Time
获取当前10位秒级时间戳。在实盘模式下返回实时时间，回测模式下返回回测时间。

返回：
- `float64` - 10位秒级时间戳

### MSToTime
将13位毫秒时间戳转换为time.Time对象。

参数：
- `timeMSecs int64` - 13位毫秒时间戳

返回：
- `*time.Time` - 时间对象指针

### Now
获取当前UTC时间。在实盘模式下返回实时时间，回测模式下返回回测时间。

返回：
- `*time.Time` - 时间对象指针

### ParseTimeMS
将时间字符串转换为13位毫秒时间戳。

支持以下时间格式：
- 年份(2006)
- 年月日(20060102)
- 10位秒级时间戳
- 13位毫秒时间戳
- 年月日时分(2006-01-02 15:04)
- 年月日时分秒(2006-01-02 15:04:05)

参数：
- `timeStr string` - 时间字符串

返回：
- `int64` - 13位毫秒时间戳
- `error` - 解析失败；使用时间戳前必须检查

### ParseTimeMSBy
根据指定的时间格式解析时间字符串为13位毫秒时间戳。

参数：
- `layout string` - 时间格式模板
- `timeStr string` - 时间字符串

返回：
- `int64` - 13位毫秒时间戳
- `error` - 解析失败；使用时间戳前必须检查

### ToDateStr
将时间戳转换为UTC时区的时间字符串。

参数：
- `timestamp int64` - 时间戳(支持10位秒级或13位毫秒)
- `format string` - 时间格式模板(默认：2006-01-02 15:04:05)

返回：
- `string` - 格式化后的时间字符串

### ToDateStrLoc
将时间戳转换为包级 LocShow 显示时区的字符串；未设置时使用 UTC。此函数不接受 location 参数，需显式时区时使用 ToTime(ts).In(loc).Format(layout)。

参数：
- `timestamp int64` - 时间戳(支持10位秒级或13位毫秒)
- `format string` - 时间格式模板(默认：2006-01-02 15:04:05)

返回：
- `string` - 格式化后的时间字符串

### ToTime
将时间戳转换为time.Time对象。

参数：
- `timestamp int64` - 时间戳(支持10位秒级或13位毫秒)

返回：
- `time.Time` - 时间对象



## 数字工具归属

CountDigit 属于 core，不是 btime；ClockState.TimeMS 才是实例模拟时钟，UTCStamp 是真实 UTC 时间。不要在回测 callback 用包级时间 facade 代替任务 clock。
