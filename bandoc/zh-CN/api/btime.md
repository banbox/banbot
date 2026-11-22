# btime 包

btime 包提供了时间处理相关的功能。

## 运行时钟

每个 `runtime.Runtime` 持有独立的 `btime.ClockState`：回测推进自己的模拟时间，实盘读取实时 UTC 时间。任务内的业务代码应通过 Runtime 传入的 ClockState 读取或推进时间，避免不同任务互相影响。

下方的包级时间函数保留给旧调用链；它们不能作为多个 Runtime 并行运行时的任务时钟。

## ClockState 方法与回测示例

`NewClockState(backtest bool, loc *time.Location) *ClockState` 创建时钟。`TimeMS()` 返回毫秒，`Time()` 返回浮点秒，`Now()` 返回 *time.Time。SetTimeMS 设置模拟时间，AdvanceMS 返回推进后的毫秒时间，仅回测生效；实盘返回实时 UTC 毫秒，delta 被忽略。未设置回测时间时首次读取会用真实时间初始化，因此手动回放前应先设置起点。下面为宿主/测试示例，正常策略不要自行推进框架时钟。

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
- 年月(200601、2006-01)
- 年月日(20060102、2006-01-02、2006/01/02)
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



## UTC 日历与会话工具

下列函数为纯函数，参数均为 UTC 毫秒时间戳，不依赖运行时钟；需要任务时间时先用 `clock.TimeMS()` 取得。

### 日历

| 函数 | 说明 |
| --- | --- |
| `UTCCalendarParts(ms int64) (year int, month time.Month, day int, weekday time.Weekday, hour, minute int)` | 分解为 UTC 日历分量 |
| `IsLeapYearUTC(year int) bool` | 是否闰年 |
| `DaysInMonthUTC(year int, month time.Month) int` | 当月天数（28..31） |
| `DaysFromMonthStartUTC(ms int64) int` | 当月第几天，1 日为 0 |
| `DaysBeforeMonthEndUTC(ms int64) int` | 距月末天数，当月最后一天为 0 |
| `IsTurnOfMonthUTC(ms int64, lastNDays, firstMDays int) bool` | 是否在月末最后 N 天或月初前 M 天内；两者都 <=0 返回 false |
| `DayOfWeekUTC(ms int64) int` | 星期，0=周日..6=周六 |
| `ISODayOfWeekUTC(ms int64) int` | ISO 星期，1=周一..7=周日 |
| `WeekOfMonthUTC(ms int64) int` | 月内第几周（1..5），按每 7 天一周 |
| `NthWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday, n int) int` | 当月第 n 个指定星期的日号；不存在或参数非法返回 0 |
| `LastWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday) int` | 当月最后一个指定星期的日号；非法返回 0 |
| `IsNthWeekdayOfMonthUTC(ms int64, weekday time.Weekday, n int) bool` | 是否为当月第 n 个指定星期 |
| `IsMonthlyExpiryDayUTC(ms int64, weekday time.Weekday) bool` | 是否为当月最后一个指定星期（月度交割日） |
| `InMonthRangeUTC(ms int64, startMonth, endMonth int) bool` | 月份是否在闭区间内，支持跨年（如 11..4）；月份超出 1..12 返回 false |
| `IsFirstBarOfPeriodUTC(prevBarMS, curBarMS int64, period string) bool` | 当前 bar 是否跨入新的 UTC 周期。`period`：`day`/`1d`/`d`、`week`/`1w`/`w`（周一起）、`month`/`1M`/`M`、`quarter`/`1q`/`q`、`year`/`1y`/`y`；`prevBarMS<=0`、`curBarMS<=prevBarMS` 或未知周期返回 false |

```go
// 每月第一根 K 线定投
if btime.IsFirstBarOfPeriodUTC(prevBarMS, curBarMS, "month") {
    // 触发定投/再平衡
}
```

### 会话

`UTCSessionWindow{Name string; StartMin, EndMin int}` 以自 00:00 UTC 起的分钟表示窗口（`StartMin>EndMin` 跨午夜），方法 `Contains(ms int64) bool` 判断是否在窗口内，`DurationMinutes() int` 返回窗口时长（0..1440）。

预定义窗口：`SessionUTC0`（全天）、`SessionAsia`（00:00-08:00）、`SessionLondon`（07:00-16:00）、`SessionNewYork`（13:30-21:00）、`SessionUSCash`（13:30-20:00）、`SessionCME`（23:00-22:00，跨午夜）、`KillzoneAsia`（00-04）、`KillzoneLondon`（07-10）、`KillzoneNewYork`（12-15）、`KillzoneLondonExit`（15-17）。常量：`MinuteMS`、`HourMS`、`DayMS`、`MinutesPerDay`，以及锚点分钟 `UTC0Minutes`、`AsiaOpenMinutes`、`AsiaCloseMinutes`、`LondonOpenMinutes`、`LondonCashOpenMinutes`、`LondonCloseMinutes`、`NYKillzoneOpenMinutes`、`NewYorkOpenMinutes`、`USCashCloseMinutes`、`NewYorkCloseMinutes`、`CMEOpenMinutes`、`CMECloseMinutes`。

| 函数 | 说明 |
| --- | --- |
| `NormalizeDayMinutes(minutes int) int` | 取模到 [0,1439] |
| `InUTCWindow(ms int64, startMin, endMin int) bool` | 是否在 UTC 日内窗口 [startMin,endMin)，`startMin>endMin` 表示跨午夜 |
| `MinutesOfDayUTC(ms int64) int` | UTC 当日已过分钟数 [0,1439] |
| `SessionDayIndex(ms int64, anchorMinutesUTC int) int64` | 以锚点分钟为日界的交易日序号 |
| `SessionStartMS(ms int64, anchorMinutesUTC int) int64` | 所在交易日的起始毫秒 |
| `MinutesIntoSession(ms int64, anchorMinutesUTC int) int` | 自交易日起点已过分钟数 |
| `IsNearSessionEnd(barStartMS, barStopMS int64, closeMinutesUTC int) bool` | bar 区间 (start, stop] 是否包含当日收盘分钟；日线及以上周期返回 false |

```go
if btime.SessionNewYork.Contains(barMS) && !btime.IsNearSessionEnd(barMS, barMS+tfMS, btime.NewYorkCloseMinutes) {
    // 纽约时段内且本 bar 未触及收盘
}
```

## 数字工具归属

CountDigit 属于 core，不是 btime；ClockState.TimeMS 才是实例模拟时钟，UTCStamp 是真实 UTC 时间。不要在回测 callback 用包级时间 facade 代替任务 clock。
