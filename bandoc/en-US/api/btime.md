# btime Package

The btime package provides time processing functionality.

## Runtime Clock

Each `runtime.Runtime` owns an independent `btime.ClockState`: a backtest advances its own simulated time, while live trading reads real UTC time. Business code inside a task should receive that ClockState through the Runtime and use it to read or advance time, avoiding interference between tasks.

The package-level time functions below remain for legacy call chains; they are not a task clock for several Runtimes running in parallel.

## ClockState methods and replay example

`NewClockState(backtest bool, loc *time.Location) *ClockState` constructs a clock. `TimeMS()` returns milliseconds, `Time()` floating-point seconds, and `Now()` *time.Time. SetTimeMS sets simulation time; AdvanceMS returns the advanced millisecond timestamp; it only takes effect in backtests, and in live mode it returns real UTC milliseconds and ignores delta. An unset replay clock initializes from real time on its first read, so set the start first. This is a host/test example; normal strategies must not advance the framework clock themselves.

```go
clock := btime.NewClockState(true, time.UTC)
startMS, err := btime.ParseTimeMS("2025-01-01 00:00:00")
if err != nil { panic(err) }
clock.SetTimeMS(startMS)
nextMS := clock.AdvanceMS(60_000)
fmt.Println(nextMS - startMS) // 60000
```

## Public Methods

### TimeMS
Get the compatibility clock timestamp in milliseconds: simulation time in backtests, real time in live mode.

Returns:
- `int64` - Current timestamp

### UTCTime
Get current UTC time as 10-digit floating-point seconds timestamp.

Returns:
- `float64` - 10-digit floating-point seconds timestamp

### UTCStamp
Get current UTC time as 13-digit milliseconds timestamp.

Returns:
- `int64` - 13-digit milliseconds timestamp

### Time
Get current 10-digit seconds timestamp. Returns real-time in live mode, backtest time in backtest mode.

Returns:
- `float64` - 10-digit seconds timestamp

### MSToTime
Convert 13-digit milliseconds timestamp to time.Time object.

Parameters:
- `timeMSecs int64` - 13-digit milliseconds timestamp

Returns:
- `*time.Time` - Time object pointer

### Now
Get current UTC time. Returns real-time in live mode, backtest time in backtest mode.

Returns:
- `*time.Time` - Time object pointer

### ParseTimeMS
Convert time string to 13-digit milliseconds timestamp.

Supports following time formats:
- Year (2006)
- Year-Month (200601, 2006-01)
- Year-Month-Day (20060102, 2006-01-02, 2006/01/02)
- 10-digit seconds timestamp
- 13-digit milliseconds timestamp
- Year-Month-Day Hour:Minute (2006-01-02 15:04)
- Year-Month-Day Hour:Minute:Second (2006-01-02 15:04:05)

Parameters:
- `timeStr string` - Time string

Returns:
- `int64` - 13-digit milliseconds timestamp
- `error` - Parse failure; check before using the timestamp

### ParseTimeMSBy
Parse time string to 13-digit milliseconds timestamp according to specified time format.

Parameters:
- `layout string` - Time format template
- `timeStr string` - Time string

Returns:
- `int64` - 13-digit milliseconds timestamp
- `error` - Parse failure; check before using the timestamp

### ToDateStr
Convert timestamp to UTC timezone time string.

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)
- `format string` - Time format template (default: 2006-01-02 15:04:05)

Returns:
- `string` - Formatted time string

### ToDateStrLoc
Format in package-level LocShow (UTC when unset). This function does not accept a location argument; for an explicit timezone use ToTime(ts).In(loc).Format(layout).

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)
- `format string` - Time format template (default: 2006-01-02 15:04:05)

Returns:
- `string` - Formatted time string

### ToTime
Convert timestamp to time.Time object.

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)

Returns:
- `time.Time` - Time object



## UTC Calendar and Session Helpers

These are pure functions taking UTC millisecond timestamps; they do not depend on the runtime clock. To use task time, get it first with `clock.TimeMS()`.

### Calendar

| Function | Description |
| --- | --- |
| `UTCCalendarParts(ms int64) (year int, month time.Month, day int, weekday time.Weekday, hour, minute int)` | Split into UTC calendar parts |
| `IsLeapYearUTC(year int) bool` | Whether the year is a leap year |
| `DaysInMonthUTC(year int, month time.Month) int` | Days in the month (28..31) |
| `DaysFromMonthStartUTC(ms int64) int` | Days since the start of the month; the 1st is 0 |
| `DaysBeforeMonthEndUTC(ms int64) int` | Days until month end; the last day is 0 |
| `IsTurnOfMonthUTC(ms int64, lastNDays, firstMDays int) bool` | Whether the day is within the last N or first M days of the month; false if both are <=0 |
| `DayOfWeekUTC(ms int64) int` | Weekday, 0=Sunday..6=Saturday |
| `ISODayOfWeekUTC(ms int64) int` | ISO weekday, 1=Monday..7=Sunday |
| `WeekOfMonthUTC(ms int64) int` | Week of the month (1..5), counted in blocks of 7 days |
| `NthWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday, n int) int` | Day of the month of the n-th given weekday; 0 if absent or invalid |
| `LastWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday) int` | Day of the month of the last given weekday; 0 if invalid |
| `IsNthWeekdayOfMonthUTC(ms int64, weekday time.Weekday, n int) bool` | Whether the day is the n-th given weekday of its month |
| `IsMonthlyExpiryDayUTC(ms int64, weekday time.Weekday) bool` | Whether the day is the last given weekday of its month (monthly expiry) |
| `InMonthRangeUTC(ms int64, startMonth, endMonth int) bool` | Whether the month is within the closed range, wrapping across years (e.g. 11..4); false outside 1..12 |
| `IsFirstBarOfPeriodUTC(prevBarMS, curBarMS int64, period string) bool` | Whether the current bar crosses into a new UTC period. `period`: `day`/`1d`/`d`, `week`/`1w`/`w` (Monday start), `month`/`1M`/`M`, `quarter`/`1q`/`q`, `year`/`1y`/`y`; false if `prevBarMS<=0`, `curBarMS<=prevBarMS` or the period is unknown |

```go
// Dollar-cost average on the first bar of each month
if btime.IsFirstBarOfPeriodUTC(prevBarMS, curBarMS, "month") {
    // trigger DCA / rebalance
}
```

### Sessions

`UTCSessionWindow{Name string; StartMin, EndMin int}` describes a window in minutes since 00:00 UTC (`StartMin>EndMin` wraps midnight). `Contains(ms int64) bool` tests membership and `DurationMinutes() int` returns the window length (0..1440).

Predefined windows: `SessionUTC0` (all day), `SessionAsia` (00:00-08:00), `SessionLondon` (07:00-16:00), `SessionNewYork` (13:30-21:00), `SessionUSCash` (13:30-20:00), `SessionCME` (23:00-22:00, wraps midnight), `KillzoneAsia` (00-04), `KillzoneLondon` (07-10), `KillzoneNewYork` (12-15), `KillzoneLondonExit` (15-17). Constants: `MinuteMS`, `HourMS`, `DayMS`, `MinutesPerDay`, plus anchor minutes `UTC0Minutes`, `AsiaOpenMinutes`, `AsiaCloseMinutes`, `LondonOpenMinutes`, `LondonCashOpenMinutes`, `LondonCloseMinutes`, `NYKillzoneOpenMinutes`, `NewYorkOpenMinutes`, `USCashCloseMinutes`, `NewYorkCloseMinutes`, `CMEOpenMinutes`, `CMECloseMinutes`.

| Function | Description |
| --- | --- |
| `NormalizeDayMinutes(minutes int) int` | Wrap minutes into [0,1439] |
| `InUTCWindow(ms int64, startMin, endMin int) bool` | Whether the time is in the UTC intraday window [startMin,endMin); `startMin>endMin` wraps midnight |
| `MinutesOfDayUTC(ms int64) int` | Minutes elapsed in the UTC day [0,1439] |
| `SessionDayIndex(ms int64, anchorMinutesUTC int) int64` | Session-day index with the day boundary at the anchor minute |
| `SessionStartMS(ms int64, anchorMinutesUTC int) int64` | Start (ms) of the session day containing `ms` |
| `MinutesIntoSession(ms int64, anchorMinutesUTC int) int` | Minutes elapsed since the session-day start |
| `IsNearSessionEnd(barStartMS, barStopMS int64, closeMinutesUTC int) bool` | Whether the bar interval (start, stop] contains the daily close minute; false for daily bars and above |

```go
if btime.SessionNewYork.Contains(barMS) && !btime.IsNearSessionEnd(barMS, barMS+tfMS, btime.NewYorkCloseMinutes) {
    // inside the New York session and this bar does not reach the close
}
```

## Digit helpers and clocks

CountDigit belongs to core, not btime. ClockState.TimeMS is task simulation time; UTCStamp is real UTC time. Backtest callbacks should use their task clock rather than package-level compatibility time.
